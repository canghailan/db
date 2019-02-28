package cc.whohow.db;

import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

public class DateTime {
    private static final DateTimeFormatter ISO_8601 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private static final Pattern DATE_TIME = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}\\+\\d{4}");

    public static DateTimeFormatter iso8601() {
        return ISO_8601;
    }

    public static boolean isDateTime(String dateTime) {
        return DATE_TIME.matcher(dateTime).matches();
    }
}
