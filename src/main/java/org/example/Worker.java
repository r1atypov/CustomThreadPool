package org.example;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Worker implements Runnable {
    private final CustomThreadPoolExecutor pool;
    private final BlockingQueue<Runnable> queue;
    private final long keepAliveTimeNanos;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public Worker(CustomThreadPoolExecutor pool,
                  BlockingQueue<Runnable> queue,
                  long keepAliveTime,
                  TimeUnit unit) {
        this.pool = pool;
        this.queue = queue;
        this.keepAliveTimeNanos = unit.toNanos(keepAliveTime);
    }

    @Override
    public void run() {
        String name = Thread.currentThread().getName();
        try {
            while (running.get()) {
                if (pool.isShutdownRequested()) {
                    break;
                }

                Runnable task = queue.poll(keepAliveTimeNanos, TimeUnit.NANOSECONDS);
                if (task == null) {
                    // idle timeout
                    if (pool.canWorkerExit()) {
                        System.out.println("[Worker] " + name + " idle timeout, stopping.");
                        break;
                    } else {
                        // нельзя уходить ниже corePoolSize — продолжаем ждать
                        continue;
                    }
                }

                System.out.println("[Worker] " + name + " executes " + task);
                try {
                    task.run();
                } catch (Throwable t) {
                    System.out.println("[Worker] " + name + " task threw exception: " + t);
                }
            }
        } catch (InterruptedException e) {
            // shutdownNow или прерывание
            System.out.println("[Worker] " + name + " interrupted, stopping.");
        } finally {
            System.out.println("[Worker] " + name + " terminated.");
            pool.onWorkerExit(this);
        }
    }

    public void stop() {
        running.set(false);
    }
}