package cc.whohow.db;

public abstract class CloseRunnable implements AutoCloseable, Runnable {
    private static final CloseRunnable EMPTY = new Empty();

    public static CloseRunnable builder() {
        return builder(empty());
    }

    public static CloseRunnable builder(CloseRunnable closeRunnable) {
        return new Builder(closeRunnable == null ? empty() : closeRunnable);
    }

    public static CloseRunnable empty() {
        return EMPTY;
    }

    public static CloseRunnable of(Runnable runnable) {
        return new RunnableAdapter(runnable);
    }

    public static CloseRunnable of(AutoCloseable closeable) {
        return new CloseableAdapter(closeable);
    }

    public CloseRunnable compose(Runnable runnable) {
        return (this == EMPTY) ? of(runnable) : new Composite(runnable, this);
    }

    public CloseRunnable compose(AutoCloseable closeable) {
        return (this == EMPTY) ? of(closeable) : new Composite(of(closeable), this);
    }

    public CloseRunnable andThen(Runnable runnable) {
        return (this == EMPTY) ? of(runnable) : new Composite(this, runnable);
    }

    public CloseRunnable andThen(AutoCloseable closeable) {
        return (this == EMPTY) ? of(closeable) : new Composite(this, of(closeable));
    }

    @Override
    public void close() {
        run();
    }

    private static class Empty extends CloseRunnable {
        @Override
        public void run() {
        }
    }

    private static class RunnableAdapter extends CloseRunnable {
        private final Runnable runnable;

        private RunnableAdapter(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } catch (Throwable ignore) {
            }
        }
    }

    private static class CloseableAdapter extends CloseRunnable {
        private final AutoCloseable closeable;

        private CloseableAdapter(AutoCloseable closeable) {
            this.closeable = closeable;
        }

        @Override
        public void run() {
            try {
                closeable.close();
            } catch (Throwable ignore) {
            }
        }
    }

    private static class Composite extends CloseRunnable {
        private final Runnable runnable1;
        private final Runnable runnable2;

        private Composite(Runnable runnable1, Runnable runnable2) {
            this.runnable1 = runnable1;
            this.runnable2 = runnable2;
        }

        @Override
        public void run() {
            try {
                runnable1.run();
            } catch (Throwable ignore) {
            }
            try {
                runnable2.run();
            } catch (Throwable ignore) {
            }
        }
    }

    private static class Builder extends CloseRunnable {
        private CloseRunnable closeRunnable;

        public Builder(CloseRunnable closeRunnable) {
            this.closeRunnable = closeRunnable;
        }

        @Override
        public CloseRunnable compose(Runnable runnable) {
            closeRunnable = closeRunnable.compose(runnable);
            return this;
        }

        @Override
        public CloseRunnable compose(AutoCloseable closeable) {
            closeRunnable = closeRunnable.compose(closeable);
            return this;
        }

        @Override
        public CloseRunnable andThen(Runnable runnable) {
            closeRunnable = closeRunnable.andThen(runnable);
            return this;
        }

        @Override
        public CloseRunnable andThen(AutoCloseable closeable) {
            closeRunnable = closeRunnable.andThen(closeable);
            return this;
        }

        @Override
        public void run() {
            closeRunnable.run();
        }
    }
}
