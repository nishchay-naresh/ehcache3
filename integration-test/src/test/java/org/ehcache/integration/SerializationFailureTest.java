package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.Builder;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.serialization.PlainJavaSerializer;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.persistence;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@RunWith(Parameterized.class)
public class SerializationFailureTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return asList(new Object[][] {
      { newResourcePoolsBuilder().heap(100, ENTRIES) },
      { newResourcePoolsBuilder().offheap(5, MB) },
      { newResourcePoolsBuilder().disk(10, MB) },

      { newResourcePoolsBuilder().heap(100, ENTRIES).offheap(5, MB) },
      { newResourcePoolsBuilder().heap(100, ENTRIES).disk(10, MB) },

      { newResourcePoolsBuilder().heap(100, ENTRIES).offheap(5, MB).disk(10, MB) },
    });
  }

  private final ResourcePools resources;

  @Rule
  public final TemporaryFolder diskPath = new TemporaryFolder();

  public SerializationFailureTest(Builder<? extends ResourcePools> resources) {
    this.resources = resources.build();
  }

  @Test
  public void testStoresTolerateSerializerFailures() throws IOException, ClassNotFoundException {
    Serializer<String> serializer = Mockito.spy(new PlainJavaSerializer<>(SerializationFailureTest.class.getClassLoader()));

    doAnswer(invocation -> {
      throw new RuntimeException();
    }).when(serializer).read(any(ByteBuffer.class));

    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().with(persistence(diskPath.newFolder())).build(true)) {
      Cache<String, String> cache = cacheManager.createCache("test", newCacheConfigurationBuilder(String.class, String.class, resources).withValueSerializer(serializer));

      cache.put("foo", "bar");

      cache.get("foo");
    }
  }

  @Test
  public void testStoresTolerateSerializerFailuresMultiThreaded() throws IOException, ClassNotFoundException, InterruptedException {
    Serializer<String> serializer = Mockito.spy(new PlainJavaSerializer<>(SerializationFailureTest.class.getClassLoader()));

    doAnswer(invocation -> {
      throw new RuntimeException();
    }).when(serializer).read(any(ByteBuffer.class));

    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().with(persistence(diskPath.newFolder())).build(true)) {
      Cache<String, String> cache = cacheManager.createCache("test", newCacheConfigurationBuilder(String.class, String.class, resources).withValueSerializer(serializer));

      cache.put("foo", "bar");

      ExecutorService executor = Executors.newCachedThreadPool();
      try {
        List<Future<Object>> foo = executor.invokeAll(Collections.nCopies(10, () -> cache.get("foo")));

        List<Throwable> failures = foo.stream().map(f -> {
          try {
            f.get();
            return null;
          } catch (InterruptedException e) {
            throw new AssertionError(e);
          } catch (ExecutionException e) {
            return e.getCause();
          }
        }).filter(Objects::nonNull).collect(Collectors.toList());

        if (!failures.isEmpty()) {
          AssertionError failure = new AssertionError();
          failures.forEach(failure::addSuppressed);
          throw failure;
        }
      } finally {
        executor.shutdown();
      }
    }
  }
}
