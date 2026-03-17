package ratelimiter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

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

    Long currentTokens = clearListLastTime(key);

    if (currentTokens >= 1) {
      addLastTime(key, System.currentTimeMillis());
      return true;
    } else {
      return false;
    }
  }

  public Long clearListLastTime(String key) {
    long currentTokens = getCurrentTokens(key);
    long lefti = 0;
    long righti = redis.llen(key) - 1;
    
    long midi;
    long now = System.currentTimeMillis();

    while (lefti < righti) {
      midi = (righti - lefti) / 2 + lefti + 1;
      if (now - Long.parseLong(redis.lindex(key, midi)) > timeWindowSeconds * 1000) {
        lefti = midi;
      } else {
        righti = midi - 1;
      }
    }

    if (now - Long.parseLong(redis.lindex(key, lefti)) > timeWindowSeconds * 1000) {
      // даже если индексы выйдут за границу, то список всеравно останется пустым, поэтому проверку не делаю на то, что такого индекса нет
      redis.ltrim(key, lefti + 1, -1);
      return currentTokens + lefti + 1;
    }
    return currentTokens;
  }

  public Long getCurrentTokens(String key) {
    return maxRequestCount - redis.llen(key);
  }

  public boolean initializeBucket(String key) {
    long tokens = maxRequestCount - 1;
    long lastTime = System.currentTimeMillis();

    if (tokens < 0) {
      return false;
    }

    addLastTime(key, lastTime);

    return true;
  }

  public void addLastTime(String key, long lastTime) {
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
