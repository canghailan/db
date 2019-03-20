package cc.whohow.db.rdbms;

import cc.whohow.db.rdbms.query.Rows;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;

public class JdbcDumper {
    private static final Logger log = LoggerFactory.getLogger(JdbcDumper.class);
    private static final JsonFactory JSON_FACTORY = new ObjectMapper().getFactory();
    private final Rdbms rdbms;
    private final String catalog;
    private final String schema;

    public JdbcDumper(Rdbms rdbms, String catalog, String schema) {
        this.rdbms = rdbms;
        this.catalog = catalog;
        this.schema = schema;
    }

    public JsonNode dump(File file) throws SQLException, IOException {
        try (JsonGenerator json = JSON_FACTORY.createGenerator(file, JsonEncoding.UTF8)) {
            return dump(json);
        }
    }

    public JsonNode dump(OutputStream stream) throws SQLException, IOException {
        try (JsonGenerator json = JSON_FACTORY.createGenerator(stream)) {
            json.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
            return dump(json);
        }
    }

    public JsonNode dump(Writer writer) throws SQLException, IOException {
        try (JsonGenerator json = JSON_FACTORY.createGenerator(writer)) {
            json.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
            return dump(json);
        }
    }

    public JsonNode dump(JsonGenerator json) throws SQLException, IOException {
        json.setPrettyPrinter(new DumpPrettyPrinter());

        JsonNode tables = rdbms.getTables(catalog, schema);
        try (Connection connection = rdbms.getDataSource().getConnection()) {
            json.writeStartObject();
            for (JsonNode table : tables) {
                String tableName = table.path("TABLE_NAME").textValue();
                log.debug("dump {}", tableName);
                json.writeFieldName(tableName);
                json.writeStartArray();
                try (Rows query = rdbms.getRows(connection, table)) {
                    for (ObjectNode row : query) {
                        json.writeTree(row);
                    }
                }
                json.writeEndArray();
            }
            json.writeEndObject();
        }

        return JsonNodeFactory.instance.objectNode();
    }

    private static class DumpPrettyPrinter implements PrettyPrinter {
        private int level = 0;

        @Override
        public void writeRootValueSeparator(JsonGenerator gen) throws IOException {
        }

        @Override
        public void writeStartObject(JsonGenerator gen) throws IOException {
            level++;
            if (level == 1) {
                gen.writeRaw("{\n");
            } else {
                gen.writeRaw("{");
            }
        }

        @Override
        public void writeEndObject(JsonGenerator gen, int nrOfEntries) throws IOException {
            if (level == 1) {
                gen.writeRaw("\n}");
            } else {
                gen.writeRaw("}");
            }
            level--;
        }

        @Override
        public void writeObjectEntrySeparator(JsonGenerator gen) throws IOException {
            if (level == 1) {
                gen.writeRaw(",\n");
            } else {
                gen.writeRaw(",");
            }
        }

        @Override
        public void writeObjectFieldValueSeparator(JsonGenerator gen) throws IOException {
            if (level == 1) {
                gen.writeRaw(":\n");
            } else {
                gen.writeRaw(":");
            }
        }

        @Override
        public void writeStartArray(JsonGenerator gen) throws IOException {
            gen.writeRaw("[\n");
        }

        @Override
        public void writeEndArray(JsonGenerator gen, int nrOfValues) throws IOException {
            gen.writeRaw("\n]");
        }

        @Override
        public void writeArrayValueSeparator(JsonGenerator gen) throws IOException {
            gen.writeRaw(",\n");
        }

        @Override
        public void beforeArrayValues(JsonGenerator gen) throws IOException {

        }

        @Override
        public void beforeObjectEntries(JsonGenerator gen) throws IOException {

        }
    }
}
