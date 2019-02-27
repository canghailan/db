package cc.whohow.db.cli;

import cc.whohow.db.CloseRunnable;
import cc.whohow.db.Database;
import cc.whohow.db.DatabaseDumper;
import com.fasterxml.jackson.databind.JsonNode;
import com.zaxxer.hikari.HikariDataSource;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class DatabaseDumpTask implements Task {
    private final JsonNode configuration;
    private CloseRunnable closeRunnable = CloseRunnable.empty();

    public DatabaseDumpTask(JsonNode configuration) {
        this.configuration = configuration;
    }

    @Override
    public JsonNode call() throws Exception {
        return newDatabaseDumper().dump(newOutput());
    }

    private OutputStream newOutput() throws FileNotFoundException {
        String output = configuration.path("output").asText("output.txt");
        OutputStream stream = new FileOutputStream(output);
        closeRunnable.compose(stream);
        return stream;
    }

    private DatabaseDumper newDatabaseDumper() {
        JsonNode db = configuration.path("db");

        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(db.path("url").textValue());
        dataSource.setUsername(db.path("username").textValue());
        dataSource.setPassword(db.path("password").textValue());
        dataSource.setMaximumPoolSize(db.path("max").asInt(1));
        closeRunnable = closeRunnable.andThen(dataSource);

        String catalog = db.path("catalog").textValue();
        String schema = db.path("schema").textValue();
        return new DatabaseDumper(new Database(dataSource), catalog, schema);
    }

    @Override
    public void close() throws Exception {
        closeRunnable.run();
    }
}
