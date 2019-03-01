package cc.whohow.db;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.regex.Pattern;

public class ISO_8601 {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private static final Pattern PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}\\+\\d{4}");

    public static DateTimeFormatter formatter() {
        return FORMATTER;
    }

    public static ZonedDateTime parse(String text) {
        return (text == null || text.isEmpty()) ? null : ZonedDateTime.parse(text, FORMATTER);
    }

    public static String format(ZonedDateTime dateTime) {
        return dateTime == null ? null : dateTime.format(FORMATTER);
    }

    public static String format(Instant instant) {
        return (instant == null) ? null : format(ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()));
    }

    public static String format(Date date) {
        return (date == null) ? null : format(date.toInstant());
    }

    public static String format(long timestamp) {
        return format(Instant.ofEpochMilli(timestamp));
    }

    public static boolean is(String dateTime) {
        return PATTERN.matcher(dateTime).matches();
    }
}
