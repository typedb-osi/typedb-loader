package processor;

import configuration.ConfigEntryData;
import configuration.ConfigEntryProcessor;
import graql.lang.Graql;
import graql.lang.common.GraqlToken;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.constraint.ThingConstraint;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.pattern.variable.UnboundVariable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import preprocessor.RegexPreprocessor;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class ProcessorUtil {

    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");

    public static String[] tokenizeCSVStandard(String row, char fileSeparator) {
        if (row != null && !row.isEmpty()) {
            try {
                return parseCSVString(row, fileSeparator);
            } catch (IOException ioe) {
                dataLogger.warn("row: <" + row + "> does not conform to RFC4180 - escaping all quotes and trying to insert again" + ioe);
                String newRow = row.replace("\"", "\\\"");
                try {
                    return parseCSVString(newRow, fileSeparator);
                } catch (IOException ioe2) {
                    dataLogger.error("CANNOT INSERT ROW - DOES NOT CONFORM TO RFC4180 and removing quotes didn't fix the issue..." + ioe2);
                }
            }
        }
        return new String[]{""};
    }

    private static String[] parseCSVString(String string, char fileSeparator) throws IOException {
        ArrayList<CSVRecord> returnList = (ArrayList<CSVRecord>) CSVParser.parse(string, CSVFormat.RFC4180.withDelimiter(fileSeparator)).getRecords();
        String[] ret = new String[returnList.get(0).size()];
        for (int i = 0; i < returnList.get(0).size(); i++) {
            ret[i] = returnList.get(0).get(i);
        }
        return ret;
    }

    public static String cleanToken(String token) {
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

    public static ArrayList<ThingConstraint.Value<?>> generateValueConstraints(String[] tokens,
                                                                               String[] columnNames,
                                                                               int lineNumber,
                                                                               ConfigEntryData.ConceptProcessorMapping generatorMappingForAttribute,
                                                                               ConfigEntryProcessor pce) {
        ConfigEntryProcessor.ConceptProcessor attributeGenerator = pce.getAttributeGenerator(generatorMappingForAttribute.getConceptProcessor());
        int columnNameIndex = idxOf(columnNames, generatorMappingForAttribute.getColumnName());
        ArrayList<ThingConstraint.Value<?>> valueConstraints = new ArrayList<>();

        if (columnNameIndex == -1) {
            dataLogger.error("Column name: <" + generatorMappingForAttribute.getColumnName() + "> was not found in file being processed");
        } else {
            if (columnNameIndex < tokens.length &&
                    tokens[columnNameIndex] != null &&
                    !cleanToken(tokens[columnNameIndex]).isEmpty()) {
                String cleanedToken = cleanToken(tokens[columnNameIndex]);

                if (generatorMappingForAttribute.getListSeparator() != null) {
                    for (String exploded : cleanedToken.split(generatorMappingForAttribute.getListSeparator())) {
                        String cleanedExplodedToken = cleanToken(exploded);
                        if (!cleanedExplodedToken.isEmpty()) {
                            ThingConstraint.Value<?> hasConstraint = generateValueConstraint(attributeGenerator.getAttributeType(), attributeGenerator.getValueType(), cleanedExplodedToken, lineNumber, generatorMappingForAttribute.getPreprocessor());
                            if (hasConstraint != null) {
                                valueConstraints.add(hasConstraint);
                            }
                        }
                    }
                } else {
                    ThingConstraint.Value<?> hasConstraint = generateValueConstraint(attributeGenerator.getAttributeType(), attributeGenerator.getValueType(), cleanedToken, lineNumber, generatorMappingForAttribute.getPreprocessor());
                    if (hasConstraint != null) {
                        valueConstraints.add(hasConstraint);
                    }
                }
            }
        }
        return valueConstraints;
    }

    public static ArrayList<ThingConstraint.Has> generateHasConstraint(String[] tokens,
                                                                       String[] columnNames,
                                                                       int lineNumber,
                                                                       ConfigEntryData.ConceptProcessorMapping generatorMappingForAttribute,
                                                                       ConfigEntryProcessor pce) {
        ArrayList<ThingConstraint.Has> hasConstraints = new ArrayList<>();
        ArrayList<ThingConstraint.Value<?>> valueConstraints = generateValueConstraints(tokens, columnNames, lineNumber, generatorMappingForAttribute, pce);

        String attributeGeneratorKey = generatorMappingForAttribute.getConceptProcessor();
        ConfigEntryProcessor.ConceptProcessor attributeGenerator = pce.getAttributeGenerator(attributeGeneratorKey);
        String attributeSchemaType = attributeGenerator.getAttributeType();
        for (ThingConstraint.Value<?> valueConstraint : valueConstraints) {
            hasConstraints.add(valueToHasConstraint(attributeSchemaType, valueConstraint));
        }
        return hasConstraints;
    }

    public static ThingConstraint.Has valueToHasConstraint(String attributeSchemaType, ThingConstraint.Value<?> valueConstraints) {
        if (valueConstraints != null) {
            return new ThingConstraint.Has(attributeSchemaType, valueConstraints);
        }
        return null;
    }


    public static ThingConstraint.Value<?> generateValueConstraint(String attributeSchemaType,
                                                                   AttributeValueType attributeValueType,
                                                                   String cleanedValue,
                                                                   int lineNumber,
                                                                   ConfigEntryData.ConceptProcessorMapping.PreprocessorConfig preprocessorConfig) {
        if (preprocessorConfig != null) {
            cleanedValue = applyPreprocessor(cleanedValue, preprocessorConfig);
        }

        ThingConstraint.Value<?> constraint = null;
        switch (attributeValueType) {
            case STRING:
                constraint = new ThingConstraint.Value.String(GraqlToken.Predicate.Equality.EQ, cleanedValue);
                break;
            case LONG:
                try {
                    constraint = new ThingConstraint.Value.Long(GraqlToken.Predicate.Equality.EQ, Integer.parseInt(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    dataLogger.warn(String.format("row < %s > has column of type <long> for variable < %s > with non-<long> value - skipping column - < %s >", lineNumber, attributeSchemaType, numberFormatException.getMessage()));
                }
                break;
            case DOUBLE:
                try {
                    constraint = new ThingConstraint.Value.Double(GraqlToken.Predicate.Equality.EQ, Double.parseDouble(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    dataLogger.warn(String.format("row < %s > has column of type <double> for variable < %s > with non-<double> value - skipping column - < %s >", lineNumber, attributeSchemaType, numberFormatException.getMessage()));
                }
                break;
            case BOOLEAN:
                if (cleanedValue.equalsIgnoreCase("true")) {
                    constraint = new ThingConstraint.Value.Boolean(GraqlToken.Predicate.Equality.EQ, true);
                } else if (cleanedValue.equalsIgnoreCase("false")) {
                    constraint = new ThingConstraint.Value.Boolean(GraqlToken.Predicate.Equality.EQ, false);
                } else {
                    dataLogger.warn(String.format("row < %s > has column of type <boolean> for variable < %s > with non-<boolean> value - skipping column", lineNumber, attributeSchemaType));
                }
                break;
            case DATETIME:
                try {
                    DateTimeFormatter isoDateFormatter = DateTimeFormatter.ISO_DATE;
                    String[] dt = cleanedValue.split("T");
                    LocalDate date = LocalDate.parse(dt[0], isoDateFormatter);
                    LocalDateTime dateTime;
                    if (dt.length > 1) {
                        LocalTime time = LocalTime.parse(dt[1], DateTimeFormatter.ISO_TIME);
                        dateTime = date.atTime(time);
                    } else {
                        dateTime = date.atStartOfDay();
                    }
                    constraint = new ThingConstraint.Value.DateTime(GraqlToken.Predicate.Equality.EQ, dateTime);
                } catch (DateTimeException dateTimeException) {
                    dataLogger.warn(String.format("row < %s > has column of type <datetime> for variable < %s > with non-<ISO 8601 format> datetime value:  - < %s >", lineNumber, attributeSchemaType, dateTimeException.getMessage()));
                }
                break;
            default:
                dataLogger.warn("row < %s > column type not valid - must be either: string, long, double, boolean, or datetime");
        }
        return constraint;
    }

    private static String applyPreprocessor(String cleanedValue,
                                            ConfigEntryData.ConceptProcessorMapping.PreprocessorConfig preprocessorConfig) {
        ConfigEntryData.ConceptProcessorMapping.PreprocessorConfig.PreprocessorParams params = preprocessorConfig.getParams();
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

    public static boolean isValidMatchAppend(ArrayList<ThingVariable<?>> matchStatements,
                                             ThingVariable<?> matchInsertInsertStatement,
                                             ConfigEntryData dce,
                                             ConfigEntryProcessor pce) {
        if (matchInsertInsertStatement == null) return false;
        StringBuilder matchStatement = new StringBuilder();
        for (Pattern st : matchStatements) {
            matchStatement.append(st.toString());
        }
        ArrayList<String> matchAttributes = new ArrayList<>();
        for (ConfigEntryData.ConceptProcessorMapping attributeMapping : dce.getMatchAttributes()) {
            String generatorKey = attributeMapping.getConceptProcessor();
            ConfigEntryProcessor.ConceptProcessor generatorEntry = pce.getAttributeGenerator(generatorKey);
            if (!matchStatement.toString().contains("has " + generatorEntry.getAttributeType())) {
                return false;
            }
            matchAttributes.add(generatorEntry.getAttributeType());
        }
        for (Map.Entry<String, ConfigEntryProcessor.ConceptProcessor> generatorEntry : pce.getRequiredAttributes().entrySet()) {
            if (!matchInsertInsertStatement.toString().contains("has " + generatorEntry.getValue().getAttributeType()) &&
                    !matchAttributes.contains(generatorEntry.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }

    public static boolean isValidDirectInsert(ThingVariable<?> statement, ConfigEntryProcessor pce) {
        if (statement == null) return false;
        if (!statement.toString().contains("isa " + pce.getSchemaType())) {
            return false;
        }
        for (Map.Entry<String, ConfigEntryProcessor.ConceptProcessor> cp : pce.getRequiredAttributes().entrySet()) {
            if (!statement.toString().contains("has " + cp.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }

    public static ThingVariable.Thing generateBoundThingVar(String schemaType,
                                                            String processor) {
        if (schemaType != null) {
            return Graql.var("e").isa(schemaType);
        } else {
            throw new IllegalArgumentException("Required field <schemaType> not set in processor " + processor);
        }
    }

    public static UnboundVariable generateUnboundVar() {
        return Graql.var("e");
    }

    public static String matchInsertQueriesToString(ArrayList<ThingVariable<?>> matchStatements,
                                                    ThingVariable<?> insertStatement) {
        StringBuilder ret = new StringBuilder();
        for (ThingVariable<?> st : matchStatements) {
            ret.append(st.toString());
        }
        if (insertStatement != null) ret.append(insertStatement);
        else ret.append("matchInsertStatement is null");
        return ret.toString();
    }

    public static ArrayList<ThingVariable<?>> generateMatchStatementsFromMatchAttributes(String[] rowTokens,
                                                                                         String[] columnNames,
                                                                                         int rowCounter,
                                                                                         ConfigEntryData dce,
                                                                                         ConfigEntryProcessor pce) {
        ArrayList<ThingVariable<?>> statements = new ArrayList<>();
        ThingVariable.Thing statement = generateBoundThingVar(pce.getSchemaType(), pce.getProcessor());
        for (ConfigEntryData.ConceptProcessorMapping cpm : dce.getAttributeProcessorMappings()) {
            if (cpm.isMatch()) {
                for (ThingConstraint.Has has : generateHasConstraint(rowTokens, columnNames, rowCounter, cpm, pce)) {
                    statement.constrain(has);
                }
            }
        }
        statements.add(statement);
        return statements;
    }

    public static ThingVariable<?> generateInsertStatementsWithoutMatchAttributes(String[] rowTokens,
                                                                                  String[] columnNames,
                                                                                  int rowCounter,
                                                                                  ConfigEntryData dce,
                                                                                  ConfigEntryProcessor pce) {
        UnboundVariable unbound = generateUnboundVar();
        ThingVariable<?> statement = null;
        for (ConfigEntryData.ConceptProcessorMapping cpm : dce.getAttributeProcessorMappings()) {
            if (!cpm.isMatch()) {
                for (ThingConstraint.Has has : generateHasConstraint(rowTokens, columnNames, rowCounter, cpm, pce)) {
                    if (statement == null) {
                        statement = unbound.constrain(has);
                    } else {
                        statement.constrain(has);
                    }
                }
            }
        }
        return statement;
    }

    public static ThingVariable.Thing generateDirectInsertStatementForThing(String[] rowTokens,
                                                                            String[] columnNames,
                                                                            int rowCounter,
                                                                            ConfigEntryData dce,
                                                                            ConfigEntryProcessor pce) {
        ThingVariable.Thing directInsertStatement = generateBoundThingVar(pce.getSchemaType(), pce.getProcessor());
        for (ConfigEntryData.ConceptProcessorMapping cpm : dce.getAttributeProcessorMappings()) {
            for (ThingConstraint.Has hasConstraint : generateHasConstraint(rowTokens, columnNames, rowCounter, cpm, pce)) {
                directInsertStatement.constrain(hasConstraint);
            }
        }
        return directInsertStatement;
    }
}
