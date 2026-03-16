package ratelimiter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Optional;

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
    Long currentTokens = getCurrentTokens(key).orElse(-1l);
    Long lastTime = peekLastTime(key).orElse(-1l);
    if (currentTokens == -1 || lastTime == -1) {
      return initializeBucket(key);
    }

    while ((System.currentTimeMillis() - lastTime) / 1000 >= timeWindowSeconds) {
      popLastTime(key);
      lastTime = peekLastTime(key).orElse(System.currentTimeMillis());
      
      currentTokens++;
    }

    currentTokens = Math.min(currentTokens, maxRequestCount);

    if (currentTokens >= 1) {
      updateTokenCount(key, currentTokens - 1);
      addlastTime(key, System.currentTimeMillis());
      return true;
    } else {
      return false;
    }
  }

  public Optional<Long> getCurrentTokens(String key) {
    String tokens = redis.get("tokens:" + key);
    return tokens == null ? Optional.ofNullable(null) : Optional.of(Long.parseLong(tokens));
  }

  public Optional<Long> popLastTime(String key) {
    String lastTime = redis.lpop("last-time:" + key);
    return lastTime == null ? Optional.ofNullable(null) : Optional.of(Long.parseLong(lastTime));
  }

  public Optional<Long> peekLastTime(String key) {
    String lastTime = redis.lindex("last-time:" + key, 0);
    return lastTime == null ? Optional.ofNullable(null) : Optional.of(Long.parseLong(lastTime));
  }

  public boolean initializeBucket(String key) {
    long tokens = maxRequestCount - 1;
    long lastTime = System.currentTimeMillis();

    if (tokens < 0) {
      return false;
    }

    updateTokenCount(key, tokens);
    addlastTime(key, lastTime);

    return true;
  }

  public void updateTokenCount(String key, long tokensCount) {
    redis.set("tokens:" + key, Long.toString(tokensCount));
  }

  public void addlastTime(String key, long lastTime) {
    redis.rpush("last-time:" + key, Long.toString(lastTime));
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
