package cc.whohow.db;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorCloser implements AutoCloseable {
    private final ExecutorService executor;
    private final Duration waitTimeout;

    public ExecutorCloser(ExecutorService executor) {
        this(executor, Duration.ofMinutes(1L));
    }

    public ExecutorCloser(ExecutorService executor, Duration waitTimeout) {
        this.executor = executor;
        this.waitTimeout = waitTimeout;
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (executor.awaitTermination(waitTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
                return;
            }
            executor.shutdownNow();
            executor.awaitTermination(waitTimeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
