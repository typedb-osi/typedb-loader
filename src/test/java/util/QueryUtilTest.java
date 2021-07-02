package util;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class QueryUtilTest {
    public static LocalDateTime getDT(String dtString) {
        DateTimeFormatter isoDateFormatter = DateTimeFormatter.ISO_DATE;
        String[] dt = dtString.split("T");
        LocalDate date = LocalDate.parse(dt[0], isoDateFormatter);
        if (dt.length > 1) {
            LocalTime time = LocalTime.parse(dt[1], DateTimeFormatter.ISO_TIME);
            return date.atTime(time);
        } else {
            return date.atStartOfDay();
        }
    }
}
