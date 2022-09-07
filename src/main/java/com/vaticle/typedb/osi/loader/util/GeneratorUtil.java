/*
 * Copyright (C) 2021 Bayer AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.vaticle.typedb.osi.loader.util;

import com.vaticle.typedb.osi.loader.config.Configuration;
import com.vaticle.typedb.osi.loader.io.FileLogger;
import com.vaticle.typedb.osi.loader.preprocessor.RegexPreprocessor;
import com.vaticle.typedb.osi.loader.type.AttributeValueType;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.common.TypeQLToken;
import com.vaticle.typeql.lang.pattern.constraint.ThingConstraint;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;

public class GeneratorUtil {

    private static final Logger dataLogger = LogManager.getLogger("com.vaticle.typedb.osi.loader.error");

    //TODO: remove this function and all the complications it provides... this would need to be either solved by a regex preprocessor, or is already solved by CSV.withIgnoreSurroundingSpaces()
    private static String cleanToken(String token) {
        String cleaned = token.replace("\"", "");
        cleaned = cleaned.replace("\\", "");
        cleaned = cleaned.trim();
        return cleaned;
    }

    public static void constrainThingWithHasAttributes(String[] row,
                                                       String[] header,
                                                       String filePath,
                                                       char fileSeparator,
                                                       ThingVariable<?> insertStatement,
                                                       Configuration.Definition.Attribute[] attributes) {
        for (Configuration.Definition.Attribute attribute : attributes) {
            ArrayList<ThingConstraint.Value<?>> constraintValues = GeneratorUtil.generateValueConstraintsConstrainingAttribute(
                    row, header, filePath, fileSeparator, attribute);
            for (ThingConstraint.Value<?> constraintValue : constraintValues) {
                insertStatement.constrain(GeneratorUtil.valueToHasConstraint(attribute.getAttribute(), constraintValue));
            }
        }
    }

    public static int getColumnIndexByName(String[] header, String column) {
        return Arrays.asList(header).indexOf(column);
    }

    public static ThingVariable.Thing generateBoundThingVar(String schemaType) {
        return TypeQL.var("e").isa(schemaType);
    }

    public static ThingConstraint.Has valueToHasConstraint(String attributeSchemaType,
                                                           ThingConstraint.Value<?> valueConstraints) {
        if (valueConstraints != null) {
            return new ThingConstraint.Has(attributeSchemaType, valueConstraints);
        }
        return null;
    }

    public static ArrayList<ThingConstraint.Value<?>> generateValueConstraintsConstrainingAttribute(String[] row,
                                                                                                    String[] header,
                                                                                                    String filepath,
                                                                                                    char fileSeparator,
                                                                                                    Configuration.Definition.Attribute attribute) {

        String attributeType = attribute.getAttribute();
        AttributeValueType attributeValueType = attribute.getConceptValueType();
        String listSeparator = attribute.getListSeparator();
        Configuration.PreprocessorConfig preprocessor = attribute.getPreprocessorConfig();
        String token;

        int colIdx = GeneratorUtil.getColumnIndexByName(header, attribute.getColumn());
        if (colIdx < row.length) {
            token = row[colIdx];
        } else {
            return new ArrayList<>();
        }


        ArrayList<ThingConstraint.Value<?>> valueConstraints = new ArrayList<>();
        if (token != null && !token.isEmpty()) {
            String cleanedToken = cleanToken(token);
            if (listSeparator == null) {
                ThingConstraint.Value<?> valueConstraint = generateValueConstraint(attributeType, attributeValueType, cleanedToken, preprocessor, row, filepath, fileSeparator);
                if (valueConstraint != null) {
                    valueConstraints.add(valueConstraint);
                }
            } else {
                for (String exploded : cleanedToken.split(listSeparator)) {
                    String cleanedExplodedToken = cleanToken(exploded);
                    if (!cleanedExplodedToken.isEmpty()) {
                        ThingConstraint.Value<?> valueConstraint = generateValueConstraint(attributeType, attributeValueType, cleanedExplodedToken, preprocessor, row, filepath, fileSeparator);
                        if (valueConstraint != null) {
                            valueConstraints.add(valueConstraint);
                        }
                    }
                }
            }
        }
        return valueConstraints;
    }


    public static ThingConstraint.Value<?> generateValueConstraint(String attributeSchemaType,
                                                                   AttributeValueType attributeValueType,
                                                                   String cleanedValue,
                                                                   Configuration.PreprocessorConfig preprocessorConfig,
                                                                   String[] row,
                                                                   String filepath,
                                                                   char fileSeparator) {
        String fileName = FilenameUtils.getName(filepath);
        String fileNoExtension = FilenameUtils.removeExtension(fileName);
        String originalRow = String.join(Character.toString(fileSeparator), row);

        if (preprocessorConfig != null) {
            cleanedValue = applyPreprocessor(cleanedValue, preprocessorConfig);
        }

        ThingConstraint.Value<?> constraint = null;
        switch (attributeValueType) {
            case STRING:
                constraint = new ThingConstraint.Value.String(TypeQLToken.Predicate.Equality.EQ, cleanedValue);
                break;
            case LONG:
                try {
                    constraint = new ThingConstraint.Value.Long(TypeQLToken.Predicate.Equality.EQ, Long.parseLong(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    FileLogger.getLogger().logColumnWarnings(fileName, originalRow);
                    dataLogger.warn(String.format("column of type long for variable <%s> with non-<long> value <%s> - skipping column - faulty row written to <%s_column_type.log>", attributeSchemaType, cleanedValue, fileNoExtension));
                }
                break;
            case DOUBLE:
                try {
                    constraint = new ThingConstraint.Value.Double(TypeQLToken.Predicate.Equality.EQ, Double.parseDouble(cleanedValue));
                } catch (NumberFormatException numberFormatException) {
                    FileLogger.getLogger().logColumnWarnings(fileName, originalRow);
                    dataLogger.warn(String.format("column of type double for variable <%s> with non-<double> value <%s> - skipping column - faulty row written to <%s_column_type.log>", attributeSchemaType, cleanedValue, fileNoExtension));
                }
                break;
            case BOOLEAN:
                if (cleanedValue.equalsIgnoreCase("true")) {
                    constraint = new ThingConstraint.Value.Boolean(TypeQLToken.Predicate.Equality.EQ, true);
                } else if (cleanedValue.equalsIgnoreCase("false")) {
                    constraint = new ThingConstraint.Value.Boolean(TypeQLToken.Predicate.Equality.EQ, false);
                } else {
                    FileLogger.getLogger().logColumnWarnings(fileName, originalRow);
                    dataLogger.warn(String.format("column of type boolean for variable <%s> with non-<boolean> value <%s> - skipping column - faulty row written to <%s_column_type.log>", attributeSchemaType, cleanedValue, fileNoExtension));
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
                    constraint = new ThingConstraint.Value.DateTime(TypeQLToken.Predicate.Equality.EQ, dateTime);
                } catch (DateTimeException dateTimeException) {
                    FileLogger.getLogger().logColumnWarnings(fileName, originalRow);
                    dataLogger.warn(String.format("column of type datetime for variable <%s> with non-<ISO 8601 format> datetime value <%s> - skipping column - faulty row written to <%s_column_type.log>", attributeSchemaType, cleanedValue, fileNoExtension));
                }
                break;
            default:
                dataLogger.warn("column type not valid - must be either: string, long, double, boolean, or datetime");
        }
        return constraint;
    }

    private static String applyPreprocessor(String cleanedValue,
                                            Configuration.PreprocessorConfig preprocessorConfig) {
        Configuration.PreprocessorConfig.PreprocessorParameters params = preprocessorConfig.getParameters();
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
