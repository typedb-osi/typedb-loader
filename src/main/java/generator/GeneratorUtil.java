package generator;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import graql.lang.pattern.variable.UnboundVariable;
import graql.lang.pattern.variable.ThingVariable.Thing;
import graql.lang.pattern.variable.ThingVariable.Relation;
import graql.lang.pattern.variable.ThingVariable.Attribute;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import preprocessor.RegexPreprocessor;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public class GeneratorUtil {

    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");

    public static String cleanToken(String token) {
        //TODO - expand cleaning of other strange characters at some point
        String cleaned = token.replace("\"", "");
        cleaned = cleaned.replace("\\", "");
        cleaned = cleaned.trim();
        return cleaned;
    }

    public static void malformedRow(String row,
                                    String[] rowTokens,
                                    int numberOfColumns) throws Exception {
        if (rowTokens.length > numberOfColumns) {
            throw new Exception("malformed input row (additional separator characters found) not inserted - fix the following and restart migration: " + row);
        }
    }

    public static int idxOf(String[] headerTokens,
                            String columnName) {
        return Arrays.asList(headerTokens).indexOf(columnName);
    }

    public static int[] indicesOf(String[] headerTokens,
                                  String[] columnNames) {
        int[] indices = new int[columnNames.length];
        int i = 0;
        for (String columnName : columnNames) {
            indices[i] = Arrays.asList(headerTokens).indexOf(columnName);
            i++;
        }
        return indices;
    }

    public static Attribute addValue(String[] tokens,
                                 UnboundVariable statement,
                                 String[] columnNames,
                                 DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAttribute,
                                 ProcessorConfigEntry pce,
                                 DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
        String attributeGeneratorKey = generatorMappingForAttribute.getGenerator();
        ProcessorConfigEntry.ConceptGenerator attributeGenerator = pce.getAttributeGenerator(attributeGeneratorKey);
        String columnName = generatorMappingForAttribute.getColumnName();
        int columnNameIndex = idxOf(columnNames, columnName);
        Attribute att = null;

        if (columnNameIndex == -1) {
            dataLogger.error("Column name: <" + columnName + "> was not found in file being processed");
        } else {
            if ( columnNameIndex < tokens.length &&
                    tokens[columnNameIndex] != null &&
                    !cleanToken(tokens[columnNameIndex]).isEmpty()) {
                String attributeValueType = attributeGenerator.getValueType();
                String cleanedToken = cleanToken(tokens[columnNameIndex]);
                att = addAttributeValueOfType(statement, attributeValueType, cleanedToken, preprocessorConfig);
            }
        }
        return att;
    }

    public static Thing addAttribute(String[] tokens,
                                     Thing statement,
                                     String[] columnNames,
                                     DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAttribute,
                                     ProcessorConfigEntry pce,
                                     DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
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
                    !cleanToken(tokens[columnNameIndex]).isEmpty()) {
                String attributeType = attributeGenerator.getAttributeType();
                String attributeValueType = attributeGenerator.getValueType();
                String cleanedToken = cleanToken(tokens[columnNameIndex]);
                statement = cleanExplodeAdd(statement, cleanedToken, attributeType, attributeValueType, columnListSeparator, preprocessorConfig);
            }
        }
        return statement;
    }

    public static Relation addAttribute(String[] tokens,
                                     Relation statement,
                                     String[] columnNames,
                                     DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAttribute,
                                     ProcessorConfigEntry pce,
                                     DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
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
                    !cleanToken(tokens[columnNameIndex]).isEmpty()) {
                String attributeType = attributeGenerator.getAttributeType();
                String attributeValueType = attributeGenerator.getValueType();
                String cleanedToken = cleanToken(tokens[columnNameIndex]);
                statement = cleanExplodeAdd(statement, cleanedToken, attributeType, attributeValueType, columnListSeparator, preprocessorConfig);
            }
        }
        return statement;
    }

    public static Thing addAttribute(String[] tokens,
                                     UnboundVariable statement,
                                     String[] columnNames,
                                     DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAttribute,
                                     ProcessorConfigEntry pce,
                                     DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
        String attributeGeneratorKey = generatorMappingForAttribute.getGenerator();
        ProcessorConfigEntry.ConceptGenerator attributeGenerator = pce.getAttributeGenerator(attributeGeneratorKey);
        String columnListSeparator = generatorMappingForAttribute.getListSeparator();
        String columnName = generatorMappingForAttribute.getColumnName();
        int columnNameIndex = idxOf(columnNames, columnName);
        Thing returnThing = null;

        if (columnNameIndex == -1) {
            dataLogger.error("Column name: <" + columnName + "> was not found in file being processed");
        } else {
            if ( columnNameIndex < tokens.length &&
                    tokens[columnNameIndex] != null &&
                    !cleanToken(tokens[columnNameIndex]).isEmpty()) {
                String attributeType = attributeGenerator.getAttributeType();
                String attributeValueType = attributeGenerator.getValueType();
                String cleanedToken = cleanToken(tokens[columnNameIndex]);
                returnThing = cleanExplodeAdd(statement, cleanedToken, attributeType, attributeValueType, columnListSeparator, preprocessorConfig);
            }
        }
        return returnThing;
    }

    public static Thing cleanExplodeAdd(Thing statement,
                                        String cleanedToken,
                                        String conceptType,
                                        String valueType,
                                        String listSeparator,
                                        DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
        if (listSeparator != null) {
            for (String exploded: cleanedToken.split(listSeparator)) {
                String cleanedExplodedToken = cleanToken(exploded);
                if (!cleanedExplodedToken.isEmpty()) {
                    statement = addAttributeOfColumnType(statement, conceptType, valueType, cleanedExplodedToken, preprocessorConfig);
                }
            }
            return statement;
        } else {
            return addAttributeOfColumnType(statement, conceptType, valueType, cleanedToken, preprocessorConfig);
        }
    }

    public static Relation cleanExplodeAdd(Relation statement,
                                        String cleanedToken,
                                        String conceptType,
                                        String valueType,
                                        String listSeparator,
                                        DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
        if (listSeparator != null) {
            for (String exploded: cleanedToken.split(listSeparator)) {
                String cleanedExplodedToken = cleanToken(exploded);
                if (!cleanedExplodedToken.isEmpty()) {
                    statement = addAttributeOfColumnType(statement, conceptType, valueType, cleanedExplodedToken, preprocessorConfig);
                }
            }
            return statement;
        } else {
            return addAttributeOfColumnType(statement, conceptType, valueType, cleanedToken, preprocessorConfig);
        }
    }

    public static Thing cleanExplodeAdd(UnboundVariable statement,
                                        String cleanedToken,
                                        String conceptType,
                                        String valueType,
                                        String listSeparator,
                                        DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
        Thing returnThing = null;
        if (listSeparator != null) {
            int count = 0;
            for (String exploded: cleanedToken.split(listSeparator)) {
                String cleanedExplodedToken = cleanToken(exploded);
                if (!cleanedExplodedToken.isEmpty()) {
                    if (count == 0) {
                        returnThing = addAttributeOfColumnType(statement, conceptType, valueType, cleanedExplodedToken, preprocessorConfig);
                    } else {
                        returnThing = addAttributeOfColumnType(returnThing, conceptType, valueType, cleanedExplodedToken, preprocessorConfig);
                    }
                    count++;
                }
            }
            return returnThing;
        } else {
            return addAttributeOfColumnType(statement, conceptType, valueType, cleanedToken, preprocessorConfig);
        }
    }

    public static Thing addAttributeOfColumnType(Thing statement,
                                                 String conceptType,
                                                 String valueType,
                                                 String cleanedValue,
                                                 DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
        if (preprocessorConfig != null) {
            cleanedValue = applyPreprocessor(cleanedValue, preprocessorConfig);
        }

        switch (valueType) {
            case "string":
                statement = statement.has(conceptType, cleanedValue);
                break;
            case "long":
                try {
                    statement = statement.has(conceptType, Integer.parseInt(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    dataLogger.warn(String.format("current row has column of type <long> for variable < %s > with non-<long> value - skipping column", conceptType));
                    dataLogger.warn(numberFormatException.getMessage());
                }
                break;
            case "double":
                try {
                    statement = statement.has(conceptType, Double.parseDouble(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    dataLogger.warn(String.format("current row has column of type <double> for variable < %s > with non-<double> value - skipping column", conceptType));
                    dataLogger.warn(numberFormatException.getMessage());
                }
                break;
            case "boolean":
                if (cleanedValue.toLowerCase().equals("true")) {
                    statement = statement.has(conceptType, true);
                } else if (cleanedValue.toLowerCase().equals("false")) {
                    statement = statement.has(conceptType, false);
                } else {
                    dataLogger.warn(String.format("current row has column of type <boolean> for variable < %s > with non-<boolean> value - skipping column", conceptType));
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
                    dataLogger.warn(String.format("current row has column of type <datetime> for variable < %s > with non-<ISO 8601 format> datetime value: ", conceptType));
                    dataLogger.warn(dateTimeException.getMessage());
                }
                break;
            default:
                dataLogger.warn("column type not valid - must be either: string, long, double, boolean, or datetime");
                break;
        }
        return statement;
    }

    public static Relation addAttributeOfColumnType(Relation statement,
                                                 String conceptType,
                                                 String valueType,
                                                 String cleanedValue,
                                                 DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
        if (preprocessorConfig != null) {
            cleanedValue = applyPreprocessor(cleanedValue, preprocessorConfig);
        }

        switch (valueType) {
            case "string":
                statement = statement.has(conceptType, cleanedValue);
                break;
            case "long":
                try {
                    statement = statement.has(conceptType, Integer.parseInt(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    dataLogger.warn(String.format("current row has column of type <long> for variable < %s > with non-<long> value - skipping column", conceptType));
                    dataLogger.warn(numberFormatException.getMessage());
                }
                break;
            case "double":
                try {
                    statement = statement.has(conceptType, Double.parseDouble(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    dataLogger.warn(String.format("current row has column of type <double> for variable < %s > with non-<double> value - skipping column", conceptType));
                    dataLogger.warn(numberFormatException.getMessage());
                }
                break;
            case "boolean":
                if (cleanedValue.toLowerCase().equals("true")) {
                    statement = statement.has(conceptType, true);
                } else if (cleanedValue.toLowerCase().equals("false")) {
                    statement = statement.has(conceptType, false);
                } else {
                    dataLogger.warn(String.format("current row has column of type <boolean> for variable < %s > with non-<boolean> value - skipping column", conceptType));
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
                    dataLogger.warn(String.format("current row has column of type <datetime> for variable < %s > with non-<ISO 8601 format> datetime value: ", conceptType));
                    dataLogger.warn(dateTimeException.getMessage());
                }
                break;
            default:
                dataLogger.warn("column type not valid - must be either: string, long, double, boolean, or datetime");
                break;
        }
        return statement;
    }

    public static Thing addAttributeOfColumnType(UnboundVariable statement,
                                                 String conceptType,
                                                 String valueType,
                                                 String cleanedValue,
                                                 DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
        if (preprocessorConfig != null) {
            cleanedValue = applyPreprocessor(cleanedValue, preprocessorConfig);
            appLogger.debug("processor processed cleaned value: " + cleanedValue);
        }
        Thing returnThing = null;

        switch (valueType) {
            case "string":
                returnThing = statement.has(conceptType, cleanedValue);
                break;
            case "long":
                try {
                    returnThing = statement.has(conceptType, Integer.parseInt(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    dataLogger.warn(String.format("current row has column of type <long> for variable < %s > with non-<long> value - skipping column", conceptType));
                    dataLogger.warn(numberFormatException.getMessage());
                }
                break;
            case "double":
                try {
                    returnThing = statement.has(conceptType, Double.parseDouble(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    dataLogger.warn(String.format("current row has column of type <double> for variable < %s > with non-<double> value - skipping column", conceptType));
                    dataLogger.warn(numberFormatException.getMessage());
                }
                break;
            case "boolean":
                if (cleanedValue.toLowerCase().equals("true")) {
                    returnThing = statement.has(conceptType, true);
                } else if (cleanedValue.toLowerCase().equals("false")) {
                    returnThing = statement.has(conceptType, false);
                } else {
                    dataLogger.warn(String.format("current row has column of type <boolean> for variable < %s > with non-<boolean> value - skipping column", conceptType));
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
                        returnThing = statement.has(conceptType, dateTime);
                    } else {
                        LocalDateTime dateTime = date.atStartOfDay();
                        returnThing = statement.has(conceptType, dateTime);
                    }
                } catch (DateTimeException dateTimeException) {
                    dataLogger.warn(String.format("current row has column of type <datetime> for variable < %s > with non-<ISO 8601 format> datetime value: ", conceptType));
                    dataLogger.warn(dateTimeException.getMessage());
                }
                break;
            default:
                dataLogger.warn("column type not valid - must be either: string, long, double, boolean, or datetime");
                break;
        }
        return returnThing;
    }

    public static Attribute addAttributeValueOfType(UnboundVariable unboundVar,
                                                String valueType,
                                                String cleanedValue,
                                                DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
        if (preprocessorConfig != null) {
            cleanedValue = applyPreprocessor(cleanedValue, preprocessorConfig);
        }
        Attribute att = null;

        switch (valueType) {
            case "string":
                att = unboundVar.eq(cleanedValue);
                break;
            case "long":
                try {
                    att = unboundVar.eq(Integer.parseInt(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    dataLogger.warn("current row has column of type <long> with non-<long> value - skipping column");
                    dataLogger.warn(numberFormatException.getMessage());
                }
                break;
            case "double":
                try {
                    att = unboundVar.eq(Double.parseDouble(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    dataLogger.warn("current row has column of type <double> with non-<double> value - skipping column");
                    dataLogger.warn(numberFormatException.getMessage());
                }
                break;
            case "boolean":
                if (cleanedValue.toLowerCase().equals("true")) {
                    att = unboundVar.eq( true);
                } else if (cleanedValue.toLowerCase().equals("false")) {
                    att = unboundVar.eq( false);
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
                        att = unboundVar.eq(dateTime);
                    } else {
                        LocalDateTime dateTime = date.atStartOfDay();
                        att = unboundVar.eq(dateTime);
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
        return att;
    }

    private static String applyPreprocessor(String cleanedValue,
                                            DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
        DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig.PreprocessorParams params = preprocessorConfig.getParams();
        String processorType = preprocessorConfig.getType();
        switch (processorType) {
            case "regex":
                return applyRegexPreprocessor(cleanedValue, params.getRegexMatch(), params.getRegexReplace());
            default:
                throw new IllegalArgumentException("Preprocessor of type: <" + processorType + "> as specified in data config does not exist");
        }
    }

    private static String applyRegexPreprocessor(String stringToProcess,
                                                 String matchString,
                                                 String replaceString) {
        RegexPreprocessor rpp = new RegexPreprocessor(matchString, replaceString);
        return rpp.applyProcessor(stringToProcess);
    }
}
