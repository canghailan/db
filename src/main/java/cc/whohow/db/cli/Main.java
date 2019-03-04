package cc.whohow.db.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws Exception {
        try (Task task = newTask(getConfiguration(getConfOption(args)))) {
            System.out.println(task.call());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getConfOption(String[] args) {
        if (args.length == 1) {
            return args[0];
        }
        return "db-task.yml";
    }

    private static JsonNode getConfiguration(String conf) throws IOException {
        return new ObjectMapper(new YAMLFactory()).readTree(new File(conf));
    }

    private static Task newTask(JsonNode configuration) {
        switch (configuration.path("task").asText("")) {
            case "scan":
                return new JdbcScanTask(configuration);
            case "dump":
                return new JdbcDumpTask(configuration);
            case "sync":
                return new JdbcSynchronizeTask(configuration);
            default:
                throw new IllegalArgumentException(configuration.toString());
        }
    }
}
