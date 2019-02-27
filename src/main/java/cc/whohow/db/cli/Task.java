package cc.whohow.db.cli;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.concurrent.Callable;

public interface Task extends Callable<JsonNode>, AutoCloseable {
}
