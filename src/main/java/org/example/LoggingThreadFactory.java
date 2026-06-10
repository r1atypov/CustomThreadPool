package org.example;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class LoggingThreadFactory implements ThreadFactory {
    private final String poolName;
    private final AtomicInteger counter = new AtomicInteger(1);

    public LoggingThreadFactory(String poolName) {
        this.poolName = poolName;
    }

    @Override
    public Thread newThread(Runnable r) {
        String name = poolName + "-worker-" + counter.getAndIncrement();
        System.out.println("[ThreadFactory] Creating new thread: " + name);
        Thread t = new Thread(r, name);
        t.setDaemon(false);
        return t;
    }
}
