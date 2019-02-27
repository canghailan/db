package cc.whohow.db.cli;

import cc.whohow.db.CloseRunnable;
import cc.whohow.db.rdbms.Rdbms;
import cc.whohow.db.rdbms.JdbcDumper;
import com.fasterxml.jackson.databind.JsonNode;
import com.zaxxer.hikari.HikariDataSource;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class JdbcDumpTask implements Task {
    private final JsonNode configuration;
    private CloseRunnable closeRunnable = CloseRunnable.empty();

    public JdbcDumpTask(JsonNode configuration) {
        this.configuration = configuration;
    }

    @Override
    public JsonNode call() throws Exception {
        return newDumper().dump(newOutput());
    }

    private OutputStream newOutput() throws FileNotFoundException {
        String output = configuration.path("output").asText("output.txt");
        OutputStream stream = new FileOutputStream(output);
        closeRunnable = closeRunnable.andThen(stream);
        return stream;
    }

    private JdbcDumper newDumper() {
        JsonNode db = configuration.path("db");

        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(db.path("url").textValue());
        dataSource.setUsername(db.path("username").textValue());
        dataSource.setPassword(db.path("password").textValue());
        dataSource.setMaximumPoolSize(db.path("max").asInt(1));
        closeRunnable = closeRunnable.compose(dataSource);

        String catalog = db.path("catalog").textValue();
        String schema = db.path("schema").textValue();
        return new JdbcDumper(new Rdbms(dataSource), catalog, schema);
    }

    @Override
    public void close() throws Exception {
        closeRunnable.run();
    }
}
