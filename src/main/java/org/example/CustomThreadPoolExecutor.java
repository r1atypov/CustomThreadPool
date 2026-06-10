package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadPoolExecutor implements CustomExecutor {

    private final int corePoolSize;
    private final int maxPoolSize;
    private final int queueSize;
    private final int minSpareThreads;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;

    private final LoggingThreadFactory threadFactory;
    private final RejectionHandler rejectionHandler;

    private final List<Worker> workers = new ArrayList<>();
    private final List<BlockingQueue<Runnable>> queues = new ArrayList<>();

    private final AtomicInteger workerCount = new AtomicInteger(0);
    private final AtomicInteger rrIndex = new AtomicInteger(0);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public CustomThreadPoolExecutor(int corePoolSize,
                                    int maxPoolSize,
                                    int queueSize,
                                    long keepAliveTime,
                                    TimeUnit timeUnit,
                                    int minSpareThreads,
                                    String poolName,
                                    RejectionHandler rejectionHandler) {
        if (corePoolSize <= 0 || maxPoolSize < corePoolSize) {
            throw new IllegalArgumentException("Invalid pool sizes");
        }
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.queueSize = queueSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.minSpareThreads = minSpareThreads;
        this.threadFactory = new LoggingThreadFactory(poolName);
        this.rejectionHandler = rejectionHandler;

        // создаём corePoolSize воркеров и очередей
        for (int i = 0; i < corePoolSize; i++) {
            addWorker();
        }
    }

    private synchronized void addWorker() {
        if (workerCount.get() >= maxPoolSize) {
            return;
        }
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(queueSize);
        queues.add(queue);
        Worker worker = new Worker(this, queue, keepAliveTime, timeUnit);
        workers.add(worker);
        Thread t = threadFactory.newThread(worker);
        workerCount.incrementAndGet();
        t.start();
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) throw new NullPointerException("command");

        if (shutdown.get()) {
            System.out.println("[Pool] Task rejected because pool is shutting down: " + command);
            rejectionHandler.rejected(command, this);
            return;
        }

        // обеспечиваем minSpareThreads
        ensureMinSpareThreads();

        // выбираем очередь по Round Robin
        BlockingQueue<Runnable> queue = selectQueue();

        if (queue == null) {
            // нет воркеров — пробуем создать
            synchronized (this) {
                if (workerCount.get() < maxPoolSize) {
                    addWorker();
                }
            }
            queue = selectQueue();
            if (queue == null) {
                System.out.println("[Rejected] No workers available for task " + command);
                rejectionHandler.rejected(command, this);
                return;
            }
        }

        boolean offered = queue.offer(command);
        if (offered) {
            System.out.println("[Pool] Task accepted into queue #" + queues.indexOf(queue) + ": " + command);
        } else {
            System.out.println("[Rejected] Queue is full for task " + command);
            rejectionHandler.rejected(command, this);
        }
    }

    private void ensureMinSpareThreads() {
        // свободные потоки ≈ общее количество воркеров минус «примерно занятые»
        // для простоты: если workerCount < corePoolSize + minSpareThreads — пытаемся создать ещё
        int desired = corePoolSize + minSpareThreads;
        if (workerCount.get() < desired && workerCount.get() < maxPoolSize) {
            synchronized (this) {
                if (workerCount.get() < desired && workerCount.get() < maxPoolSize) {
                    addWorker();
                }
            }
        }
    }

    private BlockingQueue<Runnable> selectQueue() {
        if (queues.isEmpty()) return null;
        int idx = Math.abs(rrIndex.getAndIncrement() % queues.size());
        return queues.get(idx);
    }

    public boolean isShutdownRequested() {
        return shutdown.get();
    }

    public boolean canWorkerExit() {
        // можно завершаться, если воркеров больше corePoolSize
        return workerCount.get() > corePoolSize;
    }

    public void onWorkerExit(Worker worker) {
        synchronized (this) {
            int idx = workers.indexOf(worker);
            if (idx >= 0) {
                workers.remove(idx);
                queues.remove(idx);
                workerCount.decrementAndGet();
            }
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) throw new NullPointerException("callable");
        FutureTask<T> ft = new FutureTask<>(callable);
        execute(ft);
        return ft;
    }

    @Override
    public void shutdown() {
        System.out.println("[Pool] Shutdown requested.");
        shutdown.set(true);
        // воркеры сами завершатся после обработки очередей и таймаута
    }

    @Override
    public void shutdownNow() {
        System.out.println("[Pool] Shutdown NOW requested.");
        shutdown.set(true);
        synchronized (this) {
            for (Worker w : workers) {
                w.stop();
            }
        }
        // прерываем потоки
        Thread.currentThread().getThreadGroup().interrupt();
    }
}
