package cc.whohow.db.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.zaxxer.hikari.HikariDataSource;

import java.util.function.Function;

public class DataSourceBuilder implements Function<JsonNode, HikariDataSource> {
    @Override
    public HikariDataSource apply(JsonNode configuration) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(configuration.path("url").textValue());
        dataSource.setUsername(configuration.path("username").textValue());
        dataSource.setPassword(configuration.path("password").textValue());
        dataSource.setMaximumPoolSize(configuration.path("max").asInt(1));
        return dataSource;
    }
}
