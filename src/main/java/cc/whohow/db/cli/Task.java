package cc.whohow.db.cli;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

public interface Task extends Callable<JsonNode>, AutoCloseable, Supplier<JsonNode> {
    default JsonNode get() {
        try {
            return call();
        } catch (RuntimeException e) {
            throw e;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new UndeclaredThrowableException(e);
        } finally {
            try {
                close();
            } catch (Exception ignore) {
            }
        }
    }
}
