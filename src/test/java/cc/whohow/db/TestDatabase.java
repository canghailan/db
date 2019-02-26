package cc.whohow.db;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TestDatabase {
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
        Database database = new Database(dataSource);
        System.out.println(database.getTables());
    }

    @Test
    public void testDump() throws Exception {
        Database database = new Database(dataSource);
        new DatabaseDumper(database, "social", null).dump(System.out);
    }

    @Test
    public void testScan() throws Exception {
        Database database = new Database(dataSource);
        ExecutorService executor = new ThreadPoolExecutor(5, 5, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
        DatabaseScanner databaseScanner = new DatabaseScanner(database, executor);
        databaseScanner.setCatalogFilter(
                (catalog) -> "social".equals(catalog.path("TABLE_CAT").textValue()));
        databaseScanner.setConsumer((table, row) -> {
            if (row.path("id").asText().contains("2")) {
                System.out.println(Thread.currentThread());
                System.out.println(table.path("TABLE_NAME").textValue());
                System.out.println(row);
            }
        });
        System.out.println(databaseScanner.call());
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }
}
