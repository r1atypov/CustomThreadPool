package org.example;

public class CallerRunsRejectionHandler implements RejectionHandler {
    @Override
    public void rejected(Runnable task, CustomThreadPoolExecutor executor) {
        System.out.println("[Rejected] Task " + task + " was rejected due to overload! Executing in caller thread.");
        task.run();
    }
}
