package ratelimiter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

// Почему использовал такой алгоритм реализации RateLimiter:
// 1) Данный алгоритм позваоляет не хранить данные по поводу входа как в скользящем журнале, а удаляет их
//  не засоряя память, кроме того поиск для удаления и само удаление выполняются за O(logN + M)
//  что сопоставимо например с использованием sorted set и score в качестве времени, так как там тоже исползуется O(logN + M) для нахождения и удаления.
//  Однако при использовании sorted set реализация все таки будет быстрее на больших объемах данных чем мой со списком, однако в случаи RateLimiter
//  это не критично (например, 1000 запросов в минуту что с list что с sorted set не имеет большой разницы)
// 2) При использовании sorted set необходимо добавить что-то типо реализации генерации id в members для уникальности элементов,
//  ибо просто дублировать текущее время в score и members нельзя так как в одно и то же время может попасть несколько запросов
//  а дубликаты не запигутся и один запрос может не засчитаться
// 3) в общем мой алгоритм справляется с проблемами других алгоритмов как например дорогое хранение всех меток в скользящем журнале
// и отсутствием большой нагрузки на границах, так как кол-во токенов считается не по промежуткам а относительно текущего времени
public class RateLimiter {

  private final Jedis redis;
  private final String label;
  private final long maxRequestCount;
  private final long timeWindowSeconds;

  public RateLimiter(Jedis redis, String label, long maxRequestCount, long timeWindowSeconds) {
    this.redis = redis;
    this.label = label;
    this.maxRequestCount = maxRequestCount;
    this.timeWindowSeconds = timeWindowSeconds;
  }

  public boolean pass() {
    String key = label;
    if (!redis.exists(key) || !redis.type(key).equals("list")) {
      return initializeBucket(key);
    }

    Long currentTokens = clearStaleRequest(key);

    if (currentTokens >= 1) {
      addRequestTime(key, System.currentTimeMillis());
      return true;
    } else {
      return false;
    }
  }

  public Long clearStaleRequest(String key) {
    long leftBound = 0;
    long rightBound = redis.llen(key) - 1;
    
    long mid;
    long now = System.currentTimeMillis();

    while (leftBound < rightBound) {
      mid = (rightBound - leftBound) / 2 + leftBound + 1;
      if (now - Long.parseLong(redis.lindex(key, mid)) > timeWindowSeconds * 1000) {
        leftBound = mid;
      } else {
        rightBound = mid - 1;
      }
    }

    long currentTokens = getCurrentTokens(key);
    if (now - Long.parseLong(redis.lindex(key, leftBound)) > timeWindowSeconds * 1000) {
      // даже если индексы выйдут за границу, то список всеравно останется пустым, поэтому проверку не делаю на то, что такого индекса нет
      redis.ltrim(key, leftBound + 1, -1);
      return currentTokens + leftBound + 1;
    }
    return currentTokens;
  }

  public Long getCurrentTokens(String key) {
    return maxRequestCount - redis.llen(key);
  }

  public boolean initializeBucket(String key) {
    if (maxRequestCount - 1 < 0) {
      return false;
    }

    addRequestTime(key, System.currentTimeMillis());

    return true;
  }

  public void addRequestTime(String key, long lastTime) {
    redis.rpush(key, Long.toString(lastTime));
  }

  public static void main(String[] args) {
    JedisPool pool = new JedisPool("localhost", 6379);

    try (Jedis redis = pool.getResource()) {
      RateLimiter rateLimiter = new RateLimiter(redis, "pr_rate", 1, 1);

      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      long prev = Instant.now().toEpochMilli();
      long now;

      while (true) {
        try {
          String s = br.readLine();
          if (s == null || s.equals("q")) {
            return;
          }
          boolean passed = rateLimiter.pass();

          now = Instant.now().toEpochMilli();
          if (passed) {
            System.out.printf("%d ms: %s", now - prev, "passed");
            prev = now;
          } else {
            System.out.printf("%d ms: %s", now - prev, "limited");
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

    }
  }
}
