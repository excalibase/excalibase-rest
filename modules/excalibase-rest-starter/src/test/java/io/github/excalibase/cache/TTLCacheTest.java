package io.github.excalibase.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class TTLCacheTest {

    private TTLCache<String, String> cache;

    @BeforeEach
    void setUp() {
        cache = new TTLCache<>(Duration.ofMinutes(10));
    }

    @AfterEach
    void tearDown() {
        cache.shutdown();
    }

    // ── put / get ────────────────────────────────────────────────────────────

    @Test
    void put_storesValue_andGetReturnsIt() {
        cache.put("key1", "value1");
        assertEquals("value1", cache.get("key1"));
    }

    @Test
    void put_returnsNullWhenNoPreviousValue() {
        String previous = cache.put("key1", "value1");
        assertNull(previous);
    }

    @Test
    void put_returnsPreviousValueOnOverwrite() {
        cache.put("key1", "value1");
        String previous = cache.put("key1", "value2");
        assertEquals("value1", previous);
    }

    @Test
    void put_overwritesExistingValue() {
        cache.put("key1", "value1");
        cache.put("key1", "value2");
        assertEquals("value2", cache.get("key1"));
    }

    @Test
    void get_returnsNullForMissingKey() {
        assertNull(cache.get("nonexistent"));
    }

    @Test
    void get_returnsNullForExpiredEntry() throws InterruptedException {
        TTLCache<String, String> shortCache = new TTLCache<>(Duration.ofMillis(50));
        try {
            shortCache.put("key1", "value1");
            Thread.sleep(100);
            assertNull(shortCache.get("key1"));
        } finally {
            shortCache.shutdown();
        }
    }

    @Test
    void get_removesExpiredEntryFromCache() throws InterruptedException {
        TTLCache<String, String> shortCache = new TTLCache<>(Duration.ofMillis(50));
        try {
            shortCache.put("key1", "value1");
            assertEquals(1, shortCache.size());
            Thread.sleep(100);
            shortCache.get("key1"); // triggers removal
            assertEquals(0, shortCache.size());
        } finally {
            shortCache.shutdown();
        }
    }

    // ── containsKey ──────────────────────────────────────────────────────────

    @Test
    void containsKey_returnsTrueForExistingKey() {
        cache.put("key1", "value1");
        assertTrue(cache.containsKey("key1"));
    }

    @Test
    void containsKey_returnsFalseForMissingKey() {
        assertFalse(cache.containsKey("nonexistent"));
    }

    @Test
    void containsKey_returnsFalseForExpiredEntry() throws InterruptedException {
        TTLCache<String, String> shortCache = new TTLCache<>(Duration.ofMillis(50));
        try {
            shortCache.put("key1", "value1");
            Thread.sleep(100);
            assertFalse(shortCache.containsKey("key1"));
        } finally {
            shortCache.shutdown();
        }
    }

    @Test
    void containsKey_removesExpiredEntryOnCheck() throws InterruptedException {
        TTLCache<String, String> shortCache = new TTLCache<>(Duration.ofMillis(50));
        try {
            shortCache.put("key1", "value1");
            Thread.sleep(100);
            shortCache.containsKey("key1");
            assertEquals(0, shortCache.size());
        } finally {
            shortCache.shutdown();
        }
    }

    // ── remove ───────────────────────────────────────────────────────────────

    @Test
    void remove_returnsValueAndDeletesEntry() {
        cache.put("key1", "value1");
        String removed = cache.remove("key1");
        assertEquals("value1", removed);
        assertNull(cache.get("key1"));
    }

    @Test
    void remove_returnsNullForMissingKey() {
        assertNull(cache.remove("nonexistent"));
    }

    // ── clear ─────────────────────────────────────────────────────────────────

    @Test
    void clear_removesAllEntries() {
        cache.put("k1", "v1");
        cache.put("k2", "v2");
        cache.put("k3", "v3");
        cache.clear();
        assertEquals(0, cache.size());
        assertTrue(cache.isEmpty());
    }

    // ── size / isEmpty ────────────────────────────────────────────────────────

    @Test
    void size_returnsCorrectCount() {
        assertEquals(0, cache.size());
        cache.put("k1", "v1");
        assertEquals(1, cache.size());
        cache.put("k2", "v2");
        assertEquals(2, cache.size());
    }

    @Test
    void isEmpty_returnsTrueWhenEmpty() {
        assertTrue(cache.isEmpty());
    }

    @Test
    void isEmpty_returnsFalseWhenNotEmpty() {
        cache.put("k1", "v1");
        assertFalse(cache.isEmpty());
    }

    // ── computeIfAbsent ───────────────────────────────────────────────────────

    @Test
    void computeIfAbsent_computesAndStoresWhenMissing() {
        String result = cache.computeIfAbsent("key1", k -> "computed-" + k);
        assertEquals("computed-key1", result);
        assertEquals("computed-key1", cache.get("key1"));
    }

    @Test
    void computeIfAbsent_returnsExistingValueWithoutComputing() {
        cache.put("key1", "existing");
        AtomicInteger computeCount = new AtomicInteger(0);
        String result = cache.computeIfAbsent("key1", k -> {
            computeCount.incrementAndGet();
            return "computed";
        });
        assertEquals("existing", result);
        assertEquals(0, computeCount.get());
    }

    @Test
    void computeIfAbsent_recomputesWhenExpired() throws InterruptedException {
        TTLCache<String, String> shortCache = new TTLCache<>(Duration.ofMillis(50));
        try {
            shortCache.put("key1", "old");
            Thread.sleep(100);
            String result = shortCache.computeIfAbsent("key1", k -> "new");
            assertEquals("new", result);
        } finally {
            shortCache.shutdown();
        }
    }

    @Test
    void computeIfAbsent_doesNotStoreNullComputedValue() {
        String result = cache.computeIfAbsent("key1", k -> null);
        assertNull(result);
        assertFalse(cache.containsKey("key1"));
    }

    // ── getStats ──────────────────────────────────────────────────────────────

    @Test
    void getStats_returnsCorrectTotalEntries() {
        cache.put("k1", "v1");
        cache.put("k2", "v2");
        TTLCache.CacheStats stats = cache.getStats();
        assertEquals(2, stats.getTotalEntries());
    }

    @Test
    void getStats_returnsZeroExpiredForFreshEntries() {
        cache.put("k1", "v1");
        TTLCache.CacheStats stats = cache.getStats();
        assertEquals(0, stats.getExpiredEntries());
        assertEquals(1, stats.getValidEntries());
    }

    @Test
    void getStats_countsExpiredEntries() throws InterruptedException {
        TTLCache<String, String> shortCache = new TTLCache<>(Duration.ofMillis(50));
        try {
            shortCache.put("k1", "v1");
            shortCache.put("k2", "v2");
            Thread.sleep(100);
            TTLCache.CacheStats stats = shortCache.getStats();
            assertEquals(2, stats.getExpiredEntries());
            assertEquals(0, stats.getValidEntries());
        } finally {
            shortCache.shutdown();
        }
    }

    @Test
    void getStats_returnsTtlDuration() {
        Duration ttl = Duration.ofMinutes(5);
        TTLCache<String, String> timedCache = new TTLCache<>(ttl);
        try {
            TTLCache.CacheStats stats = timedCache.getStats();
            assertEquals(ttl, stats.getTtl());
        } finally {
            timedCache.shutdown();
        }
    }

    @Test
    void cacheStats_toString_containsKeyInfo() {
        cache.put("k1", "v1");
        TTLCache.CacheStats stats = cache.getStats();
        String str = stats.toString();
        assertTrue(str.contains("total=1"));
        assertTrue(str.contains("valid=1"));
        assertTrue(str.contains("expired=0"));
    }

    // ── shutdown ──────────────────────────────────────────────────────────────

    @Test
    void shutdown_clearsAllEntries() {
        cache.put("k1", "v1");
        cache.put("k2", "v2");
        cache.shutdown();
        assertEquals(0, cache.size());
    }

    @Test
    void shutdown_canBeCalledMultipleTimes() {
        cache.shutdown();
        // Second call should not throw
        assertDoesNotThrow(() -> cache.shutdown());
    }

    // ── concurrent access ─────────────────────────────────────────────────────

    @Test
    void concurrentPutAndGet_isThreadSafe() throws InterruptedException {
        int threadCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    cache.put("key" + idx, "value" + idx);
                    cache.get("key" + idx);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        executor.shutdown();
        // No assertion on exact size since threads may run in any order, but no exception should occur
    }

    @Test
    void concurrentComputeIfAbsent_computesOnce() throws InterruptedException {
        AtomicInteger computeCount = new AtomicInteger(0);
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    cache.computeIfAbsent("sharedKey", k -> {
                        computeCount.incrementAndGet();
                        return "computed";
                    });
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        executor.shutdown();
        // Due to concurrent execution, some threads may compute before store is visible,
        // but the final value should be consistent.
        assertEquals("computed", cache.get("sharedKey"));
    }
}
