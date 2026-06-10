package org.example;

public interface RejectionHandler {
    void rejected(Runnable task, CustomThreadPoolExecutor executor);
}

