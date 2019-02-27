package cc.whohow.db.rdbms;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.function.BiConsumer;

public class RowWriter implements BiConsumer<JsonNode, JsonNode>, Closeable {
    private final Writer writer;

    public RowWriter(Writer writer) {
        this.writer = writer;
    }

    @Override
    public void accept(JsonNode table, JsonNode row) {
        String tableName = Rdbms.getQualifiedName(
                table.path("TABLE_CAT").textValue(),
                table.path("TABLE_SCHEM").textValue(),
                table.path("TABLE_NAME").textValue());
        try {
            writer.write(tableName + "\t" + row + "\n");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        writer.flush();
        writer.close();
    }
}
