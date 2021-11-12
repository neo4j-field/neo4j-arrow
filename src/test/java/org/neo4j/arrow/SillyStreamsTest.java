package org.neo4j.arrow;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class SillyStreamsTest {

    @Test
    public void testChokingStreams() throws Exception {
        final Semaphore semaphore = new Semaphore(3);

        final AtomicInteger cnt = new AtomicInteger(0);

        int i = IntStream.range(0, 100)
                .unordered()
                .parallel()
                .map(n -> {
                    try {
                        semaphore.acquire();
                        int c = cnt.incrementAndGet();
                        System.out.printf("%d: c = %d\n", n, c);
                        cnt.decrementAndGet();
                        semaphore.release();
                        return 1;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return 0;
                    }
                }).sum();
        System.out.println("i = " + i + ", cnt = " + cnt.get());
    }

    @Disabled
    @Test
    public void testStreams() throws ExecutionException, InterruptedException {
        var n = IntStream.range(1, 1_000_000).boxed().parallel().map(i -> {
                    try {
                        Thread.sleep(50);
                    } catch (Exception e) {

                    }
                    if (i % 100 == 0)
                        System.out.printf("%s : i=%d\n", Thread.currentThread(), i);
                    return i;
                }
                ).reduce(Integer::sum);
        System.out.println(n);
    }

    @Test
    public void testStreamClosing() {

        final AtomicInteger cnt = new AtomicInteger(0);

        Stream<Long> longStream = LongStream.range(0, 36).boxed().parallel()
                .onClose(() -> System.out.println("parents stream closed!"));

        longStream.flatMap(l -> {
            cnt.incrementAndGet();
            return LongStream.range(0, l)
                    .boxed()
                    .parallel()
                    .peek(m -> System.out.println("processing " + l + ", " + m))
                    .onClose(() -> {
                        int open = cnt.decrementAndGet();
                        System.out.printf("closing substream %d (%d still open)\n", l, open);
                    });
        }).forEach(l -> System.out.println("l = " + l));
    }

    @Test
    public void testSupplier() throws Exception {
        Supplier<Long> sup = System::currentTimeMillis;
        System.out.println("sup? " + sup.get());
        Thread.sleep(1233);
        System.out.println("sup? " + sup.get());

    }

    @Test
    public void testStreamExecutors() throws Exception {
        final ThreadGroup group = new ThreadGroup("test-group");
        group.setDaemon(true);

        Executor executor = Executors.newFixedThreadPool(3, runnable -> new Thread(group, runnable));

        CompletableFuture.runAsync(() -> {
            LongStream.range(0, 33).parallel()
                    .forEach(l -> System.out.printf("%s: %d\n", Thread.currentThread(), l));

        }, executor).join();

    }
}
