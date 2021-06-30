package a_rewrite.util;

import a_rewrite.config.Configuration;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;

public class Util {
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withEscape('\\').withIgnoreSurroundingSpaces().withNullString("");
    private static final CSVFormat TSV_FORMAT = CSV_FORMAT.withDelimiter('\t').withEscape('\\').withIgnoreSurroundingSpaces().withNullString("");

    public static String[] getFileHeader(String filePath, char separator) throws IOException, IllegalArgumentException {
        BufferedReader br = newBufferedReader(filePath);
        if (separator == ',') {
            return parseCSV(br.readLine());
        } else if (separator == '\t') {
            return parseTSV(br.readLine());
        } else {
            throw new IllegalArgumentException("currently supported separators are: <,>, <\t>");
        }
    }

    public static Configuration initializeDataConfig(String dcPath) {
        try {
            BufferedReader bufferedReader = newBufferedReader(dcPath);
            Type DataConfigType = new TypeToken<Configuration>(){}.getType();
            return new Gson().fromJson(bufferedReader, DataConfigType);
        } catch (FileNotFoundException e) {
            appLogger.error("Data Configuration file not found - please check the path: " + dcPath);
            return null;
        }
    }

    public static BufferedReader newBufferedReader(String filePath) throws FileNotFoundException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
    }

    public static String[] parseBySeparator(String line, char separator) throws IOException {
        if (separator == ',') {
            return parseCSV(line);
        } else if (separator == '\t') {
            return parseTSV(line);
        } else {
            throw new IllegalArgumentException("currently supported separators are: <,>, <\t>");
        }
    }

    public static String[] parseCSV(String line) throws IOException {
        if (!line.isEmpty()) {
            CSVRecord csv = CSVParser.parse(line, CSV_FORMAT).getRecords().get(0);
            return parse(csv);
        } else {
            return new String[0];
        }
    }

    public static String[] parseTSV(String line) throws IOException {
        if (!line.isEmpty()) {
            CSVRecord tsv = CSVParser.parse(line, TSV_FORMAT).getRecords().get(0);
            return parse(tsv);
        } else {
            return new String[0];
        }
    }

    private static String[] parse(CSVRecord record) {
        String[] arr = new String[record.size()];
        for (int i = 0; i < record.size(); i++) {
            arr[i] = record.get(i);
            if (arr[i] != null) {
                if ((arr[i].equals("\\N") || arr[i].equalsIgnoreCase("null"))) arr[i] = null;
                else arr[i] = escapeDoubleQuotes(arr[i]);
            }
        }
        return arr;
    }

    public static String escapeDoubleQuotes(String toFormat) {
        return toFormat.replaceAll("\"", "\\\\\"");
    }

    public static void debug(String message,
                             Object... objects) {
        if (appLogger.isDebugEnabled()) appLogger.debug(message, objects);
    }

    public static void warn(String message,
                             Object... objects) {
        appLogger.warn(message, objects);
    }

    public static void info(String message,
                            Object... objects) {
        appLogger.info(message, objects);
    }

    public static double calculateRate(double count,
                                       Instant start,
                                       Instant end) {
        return count * Duration.ofSeconds(1).toMillis() / Duration.between(start, end).toMillis();
    }

    public static String printDuration(Instant start,
                                       Instant end) {
        return Duration.between(start, end).toString()
                .substring(2)
                .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                .toLowerCase();
    }
}
