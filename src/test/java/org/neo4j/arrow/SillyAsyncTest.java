package org.neo4j.arrow;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SillyAsyncTest {

    final static AtomicInteger cnt = new AtomicInteger(0);
    final static Runnable r = () -> {
        try {
            Thread.sleep(2000);
            System.out.printf("cnt is now %d\n", cnt.incrementAndGet());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    };

    public static void main(String[] args) throws IOException {
        final AtomicBoolean bool = new AtomicBoolean(false);
        AtomicReference<CompletableFuture<Void>> futureRef = new AtomicReference(CompletableFuture.runAsync(r));

        while (true) {
            System.out.println("Command?");
            int i = System.in.read();
            System.out.printf("i=%d\n", i);
            if (i == 10) {
                System.out.println("adding new task to future");

                futureRef.getAndUpdate(currentFuture -> currentFuture.thenRunAsync(r));
            } else {
                break;
            }
        }
        System.out.println("Waiting on future...");
        futureRef.get().join();
        System.out.println("Future complete!");
    }
}
