package org.neo4j.arrow;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class SillyStreamsTest {
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
}
