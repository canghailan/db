package cc.whohow.db;

import cc.whohow.db.mongo.MongoScanner;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class TestMongo {
    private static Properties properties;
    private static MongoClient mongoClient;

    @BeforeClass
    public static void setUp() throws Exception {
        try (InputStream stream = new FileInputStream("mongo.properties")) {
            properties = new Properties();
            properties.load(stream);

            mongoClient = new MongoClient(new MongoClientURI(properties.getProperty("uri")));
        }
    }

    @AfterClass
    public static void tearDown() {
        mongoClient.close();
    }

    @Test
    public void testScan() throws Exception {
        MongoScanner scanner = new MongoScanner(mongoClient);
        scanner.setDatabaseFilter(Predicates.include("name", "glgd"));
        scanner.setDocumentFilter(Predicates.pattern(null, "测试"));
        scanner.setConsumer((collection, document) -> {
            System.out.println(document);
        });
        System.out.println(scanner.call());
    }

    @Test
    public void testDateTime() {
        System.out.println(DateTime.isDateTime("2017-06-13T"));
        System.out.println(DateTime.isDateTime("2017-06-13T15:41:59.000+0800"));
    }
}
