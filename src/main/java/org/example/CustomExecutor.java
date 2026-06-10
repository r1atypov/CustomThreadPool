package org.example;
import java.util.concurrent.*;

public interface CustomExecutor extends Executor {
    @Override
    void execute(Runnable command);

    <T> Future<T> submit(Callable<T> callable);

    void shutdown();

    void shutdownNow();
}