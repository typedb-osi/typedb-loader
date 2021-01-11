package generator;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import graql.lang.statement.StatementInstance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public class GeneratorUtil {

    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");

    public static String cleanToken(String token) {
        String cleaned = token.replace("\"", "");
        cleaned = cleaned.replace("\\", "");
        cleaned = cleaned.trim();
        return cleaned;
    }

    public static void malformedRow(String row, String[] rowTokens, int colNum) throws Exception {
        if (rowTokens.length > colNum) {
            throw new Exception("malformed input row (additional separator characters found) not inserted - fix the following and restart migration: " + row);
        }
    }

    public static int idxOf(String[] headerTokens, DataConfigEntry.GeneratorSpecification generatorSpecification) {
        return Arrays.asList(headerTokens).indexOf(generatorSpecification.getColumnName());
    }

    public static int[] indicesOf(String[] headerTokens, DataConfigEntry.GeneratorSpecification generatorSpecification) {
        int[] indices = new int[generatorSpecification.getColumnNames().length];
        int i = 0;
        for (String column : generatorSpecification.getColumnNames()) {
            indices[i] = Arrays.asList(headerTokens).indexOf(column);
            i++;
        }
        return indices;
    }

    public static StatementInstance addAttribute(String[] tokens, StatementInstance pattern, String[] headerTokens, DataConfigEntry.GeneratorSpecification attDataConfig, ProcessorConfigEntry.ConceptGenerator attGeneratorConfig) throws Exception {
        int attDataIndex = idxOf(headerTokens, attDataConfig);
        if (attDataIndex == -1) {
            dataLogger.error("Column name: <" + attDataConfig.getColumnName() + "> was not found in file being processed");
        } else {
            if ( attDataIndex < tokens.length &&
                    tokens[attDataIndex] != null &&
                    !cleanToken(tokens[attDataIndex]).isEmpty()
            ) {
                String listSeparator = attDataConfig.getListSeparator();
                pattern = cleanExplodeAdd(pattern, tokens[attDataIndex], attGeneratorConfig.getAttributeType(), attGeneratorConfig.getValueType(), listSeparator);
            }
        }
        return pattern;
    }

    public static StatementInstance cleanExplodeAdd(StatementInstance pattern, String value, String conceptType, String valueType, String listSeparator) {
        if (listSeparator != null) {
            for (String exploded: value.split(listSeparator)) {
                String cleanedValue = cleanToken(exploded);
                if (!cleanedValue.isEmpty()) {
                    pattern = addAttributeOfColumnType(pattern, conceptType, valueType, cleanedValue);
                }
            }
            return pattern;
        } else {
            if (!cleanToken(value).isEmpty()) {
                return addAttributeOfColumnType(pattern, conceptType, valueType, cleanToken(value));
            } else {
                return pattern;
            }
        }
    }

    public static StatementInstance addAttributeOfColumnType(StatementInstance pattern, String conceptType, String valueType, String cleanedValue) {
        if (valueType.equals("string")) {
            pattern = pattern.has(conceptType, cleanedValue);
        } else if (valueType.equals("long")) {
            try {
                pattern = pattern.has(conceptType, Integer.parseInt(cleanedValue));
            } catch (NumberFormatException numberFormatException) {
                dataLogger.warn("current row has column of type <long> with non-<long> value - skipping column");
                dataLogger.warn(numberFormatException.getMessage());
            }
        } else if (valueType.equals("double")) {
            try {
                pattern = pattern.has(conceptType, Double.parseDouble(cleanedValue));
            } catch (NumberFormatException numberFormatException) {
                dataLogger.warn("current row has column of type <double> with non-<double> value - skipping column");
                dataLogger.warn(numberFormatException.getMessage());
            }
        } else if (valueType.equals("boolean")) {
            if (cleanedValue.toLowerCase().equals("true")) {
                pattern = pattern.has(conceptType, true);
            } else if (cleanedValue.toLowerCase().equals("false")) {
                pattern = pattern.has(conceptType, false);
            } else {
                dataLogger.warn("current row has column of type <boolean> with non-<boolean> value - skipping column");
            }
        }
        else if (valueType.equals("datetime")) {
            try {
                DateTimeFormatter isoDateFormatter = DateTimeFormatter.ISO_DATE;
                String[] dt = cleanedValue.split("T");
                LocalDate date = LocalDate.parse(dt[0], isoDateFormatter);
                if (dt.length > 1) {
                    LocalTime time = LocalTime.parse(dt[1], DateTimeFormatter.ISO_TIME);
                    LocalDateTime dateTime = date.atTime(time);
                    pattern = pattern.has(conceptType, dateTime);
                } else {
                    LocalDateTime dateTime = date.atStartOfDay();
                    pattern = pattern.has(conceptType, dateTime);
                }
            } catch (DateTimeException dateTimeException) {
                dataLogger.warn("current row has column of type <datetime> with non-<ISO 8601 format> datetime value: ");
                dataLogger.warn(dateTimeException.getMessage());
            }
        }
        else {
            dataLogger.warn("column type not valid - must be either: string, long, double, boolean, or datetime");
        }
        return pattern;
    }
}
