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

package com.vaticle.typedb.osi.loader.generator;

import com.vaticle.typedb.client.api.TypeDBTransaction;
import com.vaticle.typedb.client.api.answer.ConceptMap;
import com.vaticle.typedb.client.common.exception.TypeDBClientException;
import com.vaticle.typedb.osi.loader.config.Configuration;
import com.vaticle.typedb.osi.loader.io.FileLogger;
import com.vaticle.typedb.osi.loader.util.GeneratorUtil;
import com.vaticle.typedb.osi.loader.util.TypeDBUtil;
import com.vaticle.typedb.osi.loader.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.constraint.ThingConstraint;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;

import static com.vaticle.typedb.osi.loader.util.TypeDBUtil.safeInsert;

public class AppendAttributeOrInsertThingGenerator implements Generator {
    private static final Logger dataLogger = LogManager.getLogger("com.vaticle.typedb.osi.loader.error");
    private final String filePath;
    private final String[] header;
    private final Configuration.Generator.AppendAttributeOrInsertThing appendOrInsertConfiguration;
    private final char fileSeparator;

    public AppendAttributeOrInsertThingGenerator(String filePath, Configuration.Generator.AppendAttributeOrInsertThing appendOrInsertConfiguration, char fileSeparator) throws IOException {
        this.filePath = filePath;
        this.header = Util.getFileHeader(filePath, fileSeparator);
        this.appendOrInsertConfiguration = appendOrInsertConfiguration;
        this.fileSeparator = fileSeparator;
    }

    @Override
    public void write(TypeDBTransaction tx, String[] row, boolean allowMultiInsert) {
        String fileName = FilenameUtils.getName(filePath);
        String fileNoExtension = FilenameUtils.removeExtension(fileName);
        String originalRow = String.join(Character.toString(fileSeparator), row);

        if (row.length > header.length) {
            FileLogger.getLogger().logMalformed(fileName, originalRow);
            dataLogger.error("Malformed Row detected in <" + filePath + "> - written to <" + fileNoExtension + "_malformed.log" + ">");
        }

        TypeQLInsert appendQuery = generateMatchInsertStatement(row);
        TypeQLInsert insertQuery = generateThingInsertStatement(row);

        if (appendAttributeInsertStatementValid(appendQuery)) {
            try {
                Iterator<ConceptMap> answers = TypeDBUtil.executeMatch(tx, appendQuery);
                if (!answers.hasNext()) {
                    if (thingInsertStatementValid(insertQuery)) {
                        tx.query().insert(insertQuery);
                    } else {
                        FileLogger.getLogger().logInvalid(fileName, originalRow);
                        dataLogger.error("Invalid Row detected in <" + filePath + "> - written to <" + fileNoExtension + "_invalid.log" + "> - invalid Statement: <" + insertQuery.toString().replace("\n", " ") + ">");
                    }
                } else {
                    safeInsert(tx, appendQuery, answers, allowMultiInsert, filePath, originalRow, dataLogger);
                }
            } catch (TypeDBClientException typeDBClientException) {
                FileLogger.getLogger().logUnavailable(fileName, originalRow);
                dataLogger.error("TypeDB Unavailable - Row in <" + filePath + "> not inserted - written to <" + fileNoExtension + "_unavailable.log" + ">");
            }
        } else {
            if (thingInsertStatementValid(insertQuery)) {
                tx.query().insert(insertQuery);
            } else {
                FileLogger.getLogger().logInvalid(fileName, originalRow);
                dataLogger.error("Invalid Row detected in <" + filePath + "> - written to <" + fileNoExtension + "_invalid.log" + "> - invalid Statements: <" + appendQuery.toString().replace("\n", " ") + "> and <" + insertQuery.toString().replace("\n", " ") + ">");
            }
        }
    }

    public TypeQLInsert generateMatchInsertStatement(String[] row) {
        if (row.length > 0) {
            ThingVariable.Thing entityMatchStatement = TypeQL.var("thing")
                    .isa(appendOrInsertConfiguration.getMatch().getType());
            for (Configuration.Definition.Attribute ownershipThingGetter : appendOrInsertConfiguration.getMatch().getOwnerships()) {
                ArrayList<ThingConstraint.Value<?>> constraintValues = GeneratorUtil.generateValueConstraintsConstrainingAttribute(
                        row, header, filePath, fileSeparator, ownershipThingGetter);
                for (ThingConstraint.Value<?> constraintValue : constraintValues) {
                    entityMatchStatement.constrain(GeneratorUtil.valueToHasConstraint(ownershipThingGetter.getAttribute(), constraintValue));
                }
            }

            UnboundVariable insertUnboundVar = TypeQL.var("thing");
            ThingVariable.Thing insertStatement = null;
            for (Configuration.Definition.Attribute attributeToAppend : appendOrInsertConfiguration.getInsert().getOwnerships()) {
                ArrayList<ThingConstraint.Value<?>> constraintValues = GeneratorUtil.generateValueConstraintsConstrainingAttribute(
                        row, header, filePath, fileSeparator, attributeToAppend);
                for (ThingConstraint.Value<?> constraintValue : constraintValues) {
                    if (insertStatement == null) {
                        insertStatement = insertUnboundVar.constrain(GeneratorUtil.valueToHasConstraint(attributeToAppend.getAttribute(), constraintValue));
                    } else {
                        insertStatement.constrain(GeneratorUtil.valueToHasConstraint(attributeToAppend.getAttribute(), constraintValue));
                    }
                }
            }

            if (insertStatement != null) {
                return TypeQL.match(entityMatchStatement).insert(insertStatement);
            } else {
                return TypeQL.insert(TypeQL.var("null").isa("null").has("null", "null"));
            }
        } else {
            return TypeQL.insert(TypeQL.var("null").isa("null").has("null", "null"));
        }
    }

    public TypeQLInsert generateThingInsertStatement(String[] row) {
        if (row.length > 0) {
            ThingVariable.Thing insertStatement = GeneratorUtil.generateBoundThingVar(appendOrInsertConfiguration.getMatch().getType());

            for (Configuration.Definition.Attribute attribute : appendOrInsertConfiguration.getMatch().getOwnerships()) {
                ArrayList<ThingConstraint.Value<?>> constraintValues = GeneratorUtil.generateValueConstraintsConstrainingAttribute(
                        row, header, filePath, fileSeparator, attribute);
                for (ThingConstraint.Value<?> constraintValue : constraintValues) {
                    insertStatement.constrain(GeneratorUtil.valueToHasConstraint(attribute.getAttribute(), constraintValue));
                }
            }

            GeneratorUtil.constrainThingWithHasAttributes(row, header, filePath, fileSeparator, insertStatement, appendOrInsertConfiguration.getInsert().getOwnerships());

            return TypeQL.insert(insertStatement);
        } else {
            return TypeQL.insert(TypeQL.var("null").isa("null").has("null", "null"));
        }
    }

    public boolean appendAttributeInsertStatementValid(TypeQLInsert insert) {
        if (insert == null) return false;
        if (!insert.toString().contains("isa " + appendOrInsertConfiguration.getMatch().getType())) return false;
        for (Configuration.Definition.Attribute ownershipThingGetter : appendOrInsertConfiguration.getMatch().getOwnerships()) {
            if (!insert.toString().contains("has " + ownershipThingGetter.getAttribute())) return false;
        }
        if (appendOrInsertConfiguration.getInsert().getRequiredOwnerships() != null) {
            for (Configuration.Definition.Attribute attribute : appendOrInsertConfiguration.getInsert().getRequiredOwnerships()) {
                if (!insert.toString().contains("has " + attribute.getAttribute())) return false;
            }
        }
        return true;
    }

    public boolean thingInsertStatementValid(TypeQLInsert insert) {
        if (insert == null) return false;
        if (!insert.toString().contains("isa " + appendOrInsertConfiguration.getMatch().getType())) return false;
        for (Configuration.Definition.Attribute ownershipThingGetter : appendOrInsertConfiguration.getMatch().getOwnerships()) {
            if (ownershipThingGetter.getRequired() != null && ownershipThingGetter.getRequired()) {
                if (!insert.toString().contains(", has " + ownershipThingGetter.getAttribute())) return false;
            }
        }
        if (appendOrInsertConfiguration.getInsert().getRequiredOwnerships() != null) {
            for (Configuration.Definition.Attribute attribute : appendOrInsertConfiguration.getInsert().getRequiredOwnerships()) {
                if (!insert.toString().contains("has " + attribute.getAttribute())) return false;
            }
        }
        return true;
    }

    public char getFileSeparator() {
        return this.fileSeparator;
    }
}
