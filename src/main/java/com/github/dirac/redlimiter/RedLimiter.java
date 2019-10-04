package com.github.dirac.redlimiter;

import redis.clients.jedis.JedisPool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.*;

public class RedLimiter {

    private static final String STORED_PERMITS = "storedPermits";
    private static final String MAX_PERMITS = "maxPermits";
    private static final String STABLE_INTERVAL_MICROS = "stableIntervalMicros";
    private static final String NEXT_FREE_TICKET_MICROS = "nextFreeTicketMicros";

    private static final String SCRIPT = "RedLimiter.lua";

    private static final ConcurrentMap<String, RedLimiter> LIMITERS = new ConcurrentHashMap<>();

    private final String key;
    private final JedisPool jedisPool;
    private final String sha1;
    private double qps;
    private volatile int batchSize = 100;
    private volatile long lastMillis = 0L;
    private volatile long batchInterval = 100L;

    private void setProperties() {
        Map<String, String> limiter = new HashMap<>();
        limiter.put(STORED_PERMITS, Double.toString(qps));
        limiter.put(MAX_PERMITS, Double.toString(qps));
        limiter.put(STABLE_INTERVAL_MICROS, Double.toString(TimeUnit.SECONDS.toMicros(1L) / qps));
        limiter.put(NEXT_FREE_TICKET_MICROS, "0");
        jedisPool.getResource().hmset(key, limiter);
    }

    private AtomicInteger qpsHolder = new AtomicInteger(0);

    private RedLimiter(String key, double qps, JedisPool jedisPool, boolean setProperties) throws IOException {
        this.key = key;
        this.qps = qps;
        this.jedisPool = jedisPool;
        if (setProperties) {
            setProperties();
        }
        this.sha1 = loadScript();
    }

    private String loadScript() throws IOException {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(SCRIPT);
        Objects.requireNonNull(is);
        StringBuilder builder = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append("\n");
            }
        } finally {
            reader.close();
            is.close();
        }
        String script = builder.toString();
        return jedisPool.getResource().scriptLoad(script);
    }


    public static RedLimiter create(String key, double qps, JedisPool jedisPool) throws CreateException {
        return create(key, qps, jedisPool, false);
    }

    public static RedLimiter create(String key, double qps, JedisPool jedisPool, boolean setProperties) throws CreateException {
        AtomicReference<Throwable> t = new AtomicReference<>();
        AtomicBoolean fail = new AtomicBoolean(false);
        RedLimiter limiter = LIMITERS.computeIfAbsent(key, k -> {
            try {
                return new RedLimiter(k, qps, jedisPool, setProperties);
            } catch (IOException e) {
                fail.set(true);
                t.set(e);
            }
            return null;
        });
        if (fail.get()) {
            throw new CreateException(t.get());
        }
        return limiter;
    }

    public void setRate(double qps) {
        this.qps = qps;
        setProperties();
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setBatchInterval(long batchInterval) {
        this.batchInterval = batchInterval;
    }

    public double acquire() {
        return acquire(1D);
    }

    public double acquireLazy(int batchQps) {
        long currentMillis = System.currentTimeMillis();
        if (qpsHolder.get() >= batchSize || (currentMillis - this.lastMillis) < batchInterval) {
            int qps = qpsHolder.getAndSet(0);
            this.lastMillis = currentMillis;
            return acquire(qps);
        } else {
            qpsHolder.addAndGet(batchQps);
            return 0D;
        }
    }

    public double acquire(double qps) {
        long nowMicros = MILLISECONDS.toMicros(System.currentTimeMillis());
        long waitMicros = (long) jedisPool.getResource().evalsha(sha1, 1, key, "acquire",
                Double.toString(qps), Long.toString(nowMicros));
        double wait = 1.0 * waitMicros / SECONDS.toMicros(1L);
        sleepUninterruptibly(waitMicros, MICROSECONDS);
        return wait;
    }

    public boolean tryAcquire() {
        return tryAcquire(1D, 0L, MICROSECONDS);
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) {
        return tryAcquire(1D, timeout, unit);
    }

    public boolean tryAcquire(double qps, long timeout, TimeUnit unit) {
        long nowMicros = MILLISECONDS.toMicros(System.currentTimeMillis());
        long timeoutMicros = unit.toMicros(timeout);
        long waitMicros = (long) jedisPool.getResource().evalsha(sha1, 1, key, "tryAcquire",
                Double.toString(qps), Long.toString(nowMicros), Long.toString(timeoutMicros));
        if (waitMicros < 0) {
            return false;
        }
        sleepUninterruptibly(waitMicros, MICROSECONDS);
        return true;
    }

    // from Guava Uninterruptibles
    private static void sleepUninterruptibly(long sleepFor, TimeUnit unit) {
        boolean interrupted = false;
        try {
            long remainingNanos = unit.toNanos(sleepFor);
            long end = System.nanoTime() + remainingNanos;
            while (true) {
                try {
                    // TimeUnit.sleep() treats negative timeouts just like zero.
                    NANOSECONDS.sleep(remainingNanos);
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                    remainingNanos = end - System.nanoTime();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
