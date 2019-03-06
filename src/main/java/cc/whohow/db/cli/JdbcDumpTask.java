package cc.whohow.db.cli;

import cc.whohow.db.CloseRunnable;
import cc.whohow.db.rdbms.JdbcDumper;
import cc.whohow.db.rdbms.Rdbms;
import com.fasterxml.jackson.databind.JsonNode;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class JdbcDumpTask implements Task {
    protected final JsonNode configuration;
    protected CloseRunnable closeRunnable = CloseRunnable.builder();

    public JdbcDumpTask(JsonNode configuration) {
        this.configuration = configuration;
    }

    @Override
    public JsonNode call() throws Exception {
        return new JdbcDumper(new Rdbms(buildDataSource()), getCatalog(), getSchema()).dump(buildOutput());
    }

    protected DataSource buildDataSource() {
        return buildDataSource(configuration.path("db"));
    }

    protected DataSource buildDataSource(JsonNode db) {
        HikariDataSource dataSource = new DataSourceBuilder().apply(db);
        closeRunnable.compose(dataSource);
        return dataSource;
    }

    protected String getCatalog() {
        return configuration.path("db").path("catalog").textValue();
    }

    protected String getSchema() {
        return configuration.path("db").path("schema").textValue();
    }

    protected OutputStream buildOutput() throws IOException {
        String output = configuration.path("output").asText("output.txt");
        OutputStream stream = new FileOutputStream(output);
        closeRunnable.andThen(stream);
        return stream;
    }

    @Override
    public void close() throws Exception {
        closeRunnable.run();
    }
}
