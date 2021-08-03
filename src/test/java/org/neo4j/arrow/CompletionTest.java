package org.neo4j.arrow;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class CompletionTest {


    @Test
    public void testCompletionStuff() throws ExecutionException, InterruptedException {
        ExecutorService service = Executors.newCachedThreadPool();
        AtomicBoolean b = new AtomicBoolean(false);

        Runnable r = () -> {
            try {
                System.out.println("runner starting");
                Thread.sleep(2000);
                b.set(true);
                System.out.println("runner complete");

            } catch (Exception e) {

            }
        };

        CompletionStage<Integer> stage = CompletableFuture.supplyAsync(() -> {
            System.out.println("Starting...");
            return CompletableFuture.runAsync(r).toCompletableFuture();
        }).thenCompose((future) -> {
            future.join();
            return CompletableFuture.completedFuture(10);
        }).whenCompleteAsync((t, e) -> { System.out.println("hey now, i finished!"); });
        System.out.println("b: " + b.get());
        //CompletionStage<Integer> other = stage.whenCompleteAsync((integer, throwable) -> { System.out.println("future complete!"); }, service);
        Thread.sleep(5000);
        CompletableFuture<Integer> f = stage.toCompletableFuture();
        System.out.println("Is done? " + f.isDone());
        System.out.println("i = " + f.get());
        System.out.println("b: " + b.get());

    }
}
