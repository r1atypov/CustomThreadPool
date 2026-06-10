package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) throws Exception {
        CustomThreadPoolExecutor pool = new CustomThreadPoolExecutor(
                2,                  // corePoolSize
                4,                  // maxPoolSize
                5,                  // queueSize
                5,                  // keepAliveTime
                TimeUnit.SECONDS,
                1,                  // minSpareThreads
                "MyPool",
                new CallerRunsRejectionHandler()
        );

        List<Future<?>> futures = new ArrayList<>();

        // имитационные задачи
        for (int i = 0; i < 15; i++) {
            final int id = i;
            Runnable task = () -> {
                String name = Thread.currentThread().getName();
                System.out.println("[Task] #" + id + " started in " + name);
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException e) {
                    System.out.println("[Task] #" + id + " interrupted");
                }
                System.out.println("[Task] #" + id + " finished in " + name);
            };
            pool.execute(task);
        }

        // пример submit с результатом
        Future<Integer> sumFuture = pool.submit(() -> {
            System.out.println("[Callable] computing 1+2+3");
            Thread.sleep(1000L);
            return 1 + 2 + 3;
        });

        System.out.println("[Main] Waiting 10 seconds before shutdown...");
        Thread.sleep(10000L);

        pool.shutdown();

        // ждём немного, чтобы все задачи доработали
        Thread.sleep(10000L);

        System.out.println("[Main] Callable result: " + sumFuture.get());

        System.out.println("[Main] Demo overload: sending many tasks after shutdown...");
        for (int i = 0; i < 5; i++) {
            final int id = i;
            pool.execute(() -> System.out.println("[PostShutdownTask] " + id));
        }

        System.out.println("[Main] Done.");
    }
}
