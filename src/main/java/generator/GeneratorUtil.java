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
        //TODO - expand cleaning of other strange characters at some point
        String cleaned = token.replace("\"", "");
        cleaned = cleaned.replace("\\", "");
        cleaned = cleaned.trim();
        return cleaned;
    }

    public static void malformedRow(String row, String[] rowTokens, int numberOfColumns) throws Exception {
        if (rowTokens.length > numberOfColumns) {
            throw new Exception("malformed input row (additional separator characters found) not inserted - fix the following and restart migration: " + row);
        }
    }

    public static int idxOf(String[] headerTokens, String columnName) {
        return Arrays.asList(headerTokens).indexOf(columnName);
    }

    public static int[] indicesOf(String[] headerTokens, String[] columnNames) {
        int[] indices = new int[columnNames.length];
        int i = 0;
        for (String columnName : columnNames) {
            indices[i] = Arrays.asList(headerTokens).indexOf(columnName);
            i++;
        }
        return indices;
    }

    public static StatementInstance addAttribute(String[] tokens, StatementInstance statement, String[] columnNames, DataConfigEntry.dataConfigGeneratorMapping generatorMappingForAttribute, ProcessorConfigEntry pce) {
        String attributeGeneratorKey = generatorMappingForAttribute.getGenerator();
        ProcessorConfigEntry.ConceptGenerator attributeGenerator = pce.getAttributeGenerator(attributeGeneratorKey);
        String columnListSeparator = generatorMappingForAttribute.getListSeparator();
        String columnName = generatorMappingForAttribute.getColumnName();
        int columnNameIndex = idxOf(columnNames, columnName);

        if (columnNameIndex == -1) {
            dataLogger.error("Column name: <" + columnName + "> was not found in file being processed");
        } else {
            if ( columnNameIndex < tokens.length &&
                    tokens[columnNameIndex] != null &&
                    !cleanToken(tokens[columnNameIndex]).isEmpty()
            ) {
                String attributeType = attributeGenerator.getAttributeType();
                String attributeValueType = attributeGenerator.getValueType();
                String cleanedToken = cleanToken(tokens[columnNameIndex]);
                statement = cleanExplodeAdd(statement, cleanedToken, attributeType, attributeValueType, columnListSeparator);
            }
        }
        return statement;
    }

    public static StatementInstance cleanExplodeAdd(StatementInstance statement, String cleanedToken, String conceptType, String valueType, String listSeparator) {
        if (listSeparator != null) {
            for (String exploded: cleanedToken.split(listSeparator)) {
                String cleanedExplodedToken = cleanToken(exploded);
                if (!cleanedExplodedToken.isEmpty()) {
                    statement = addAttributeOfColumnType(statement, conceptType, valueType, cleanedExplodedToken);
                }
            }
            return statement;
        } else {
            return addAttributeOfColumnType(statement, conceptType, valueType, cleanedToken);
        }
    }

    public static StatementInstance addAttributeOfColumnType(StatementInstance statement, String conceptType, String valueType, String cleanedValue) {
        switch (valueType) {
            case "string":
                statement = statement.has(conceptType, cleanedValue);
                break;
            case "long":
                try {
                    statement = statement.has(conceptType, Integer.parseInt(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    dataLogger.warn("current row has column of type <long> with non-<long> value - skipping column");
                    dataLogger.warn(numberFormatException.getMessage());
                }
                break;
            case "double":
                try {
                    statement = statement.has(conceptType, Double.parseDouble(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    dataLogger.warn("current row has column of type <double> with non-<double> value - skipping column");
                    dataLogger.warn(numberFormatException.getMessage());
                }
                break;
            case "boolean":
                if (cleanedValue.toLowerCase().equals("true")) {
                    statement = statement.has(conceptType, true);
                } else if (cleanedValue.toLowerCase().equals("false")) {
                    statement = statement.has(conceptType, false);
                } else {
                    dataLogger.warn("current row has column of type <boolean> with non-<boolean> value - skipping column");
                }
                break;
            case "datetime":
                try {
                    DateTimeFormatter isoDateFormatter = DateTimeFormatter.ISO_DATE;
                    String[] dt = cleanedValue.split("T");
                    LocalDate date = LocalDate.parse(dt[0], isoDateFormatter);
                    if (dt.length > 1) {
                        LocalTime time = LocalTime.parse(dt[1], DateTimeFormatter.ISO_TIME);
                        LocalDateTime dateTime = date.atTime(time);
                        statement = statement.has(conceptType, dateTime);
                    } else {
                        LocalDateTime dateTime = date.atStartOfDay();
                        statement = statement.has(conceptType, dateTime);
                    }
                } catch (DateTimeException dateTimeException) {
                    dataLogger.warn("current row has column of type <datetime> with non-<ISO 8601 format> datetime value: ");
                    dataLogger.warn(dateTimeException.getMessage());
                }
                break;
            default:
                dataLogger.warn("column type not valid - must be either: string, long, double, boolean, or datetime");
                break;
        }
        return statement;
    }
}
