package cc.whohow.db;

import cc.whohow.db.rdbms.Rdbms;
import cc.whohow.db.rdbms.JdbcDumper;
import cc.whohow.db.rdbms.JdbcScanner;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TestRdbms {
    private static Properties properties;
    private static HikariDataSource dataSource;

    @BeforeClass
    public static void setUp() throws Exception {
        try (InputStream stream = new FileInputStream("jdbc.properties")) {
            properties = new Properties();
            properties.load(stream);

            dataSource = new HikariDataSource();
            dataSource.setJdbcUrl(properties.getProperty("url"));
            dataSource.setUsername(properties.getProperty("username"));
            dataSource.setPassword(properties.getProperty("password"));
            dataSource.setMaximumPoolSize(10);
        }
    }

    @AfterClass
    public static void tearDown() {
        dataSource.close();
    }

    @Test
    public void test() throws Exception {
        Rdbms rdbms = new Rdbms(dataSource);
        System.out.println(rdbms.getTables());
    }

    @Test
    public void testDump() throws Exception {
        Rdbms rdbms = new Rdbms(dataSource);
        new JdbcDumper(rdbms, "social", null).dump(System.out);
    }

    @Test
    public void testScan() throws Exception {
        Rdbms rdbms = new Rdbms(dataSource);
        ExecutorService executor = new ThreadPoolExecutor(5, 5, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
        JdbcScanner scanner = new JdbcScanner(rdbms);
        scanner.setExecutor(executor);
        scanner.setCatalogFilter(
                (catalog) -> "social".equals(catalog.path("TABLE_CAT").textValue()));
        scanner.setConsumer((table, row) -> {
            if (row.path("id").asText().contains("2")) {
                System.out.println(Thread.currentThread());
                System.out.println(table.path("TABLE_NAME").textValue());
                System.out.println(row);
            }
        });
        System.out.println(scanner.call());
        new ExecutorCloser(executor, Duration.ofMinutes(5)).close();
    }
}
