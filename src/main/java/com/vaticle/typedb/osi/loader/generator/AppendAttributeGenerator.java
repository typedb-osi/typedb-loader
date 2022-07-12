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
import com.vaticle.typedb.client.common.exception.TypeDBClientException;
import com.vaticle.typedb.osi.loader.config.Configuration;
import com.vaticle.typedb.osi.loader.io.FileLogger;
import com.vaticle.typedb.osi.loader.util.GeneratorUtil;
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

public class AppendAttributeGenerator implements Generator {
    private static final Logger dataLogger = LogManager.getLogger("com.vaticle.typedb.osi.loader.error");
    private final String filePath;
    private final String[] header;
    private final Configuration.Generator.AppendAttribute appendConfiguration;
    private final char fileSeparator;

    public AppendAttributeGenerator(String filePath, Configuration.Generator.AppendAttribute appendConfiguration, char fileSeparator) throws IOException {
        this.filePath = filePath;
        this.header = Util.getFileHeader(filePath, fileSeparator);
        this.appendConfiguration = appendConfiguration;
        this.fileSeparator = fileSeparator;
    }

    public void write(TypeDBTransaction tx,
                      String[] row) {
        String fileName = FilenameUtils.getName(filePath);
        String fileNoExtension = FilenameUtils.removeExtension(fileName);
        String originalRow = String.join(Character.toString(fileSeparator), row);

        if (row.length > header.length) {
            FileLogger.getLogger().logMalformed(fileName, originalRow);
            dataLogger.error("Malformed Row detected in <" + filePath + "> - written to <" + fileNoExtension + "_malformed.log" + ">");
        }

        TypeQLInsert statement = generateMatchInsertStatement(row);

        if (appendAttributeInsertStatementValid(statement)) {
            try {
                tx.query().insert(statement);
            } catch (TypeDBClientException typeDBClientException) {
                FileLogger.getLogger().logUnavailable(fileName, originalRow);
                dataLogger.error("TypeDB Unavailable - Row in <" + filePath + "> not inserted - written to <" + fileNoExtension + "_unavailable.log" + ">");
            }
        } else {
            FileLogger.getLogger().logInvalid(fileName, originalRow);
            dataLogger.error("Invalid Row detected in <" + filePath + "> - written to <" + fileNoExtension + "_invalid.log" + "> - invalid Statement: <" + statement.toString().replace("\n", " ") + ">");
        }
    }

    public TypeQLInsert generateMatchInsertStatement(String[] row) {
        if (row.length > 0) {
            ThingVariable.Thing entityMatchStatement = TypeQL.var("thing")
                    .isa(appendConfiguration.getMatch().getType());
            for (Configuration.Definition.Attribute consAtt : appendConfiguration.getMatch().getOwnerships()) {
                ArrayList<ThingConstraint.Value<?>> constraintValues = GeneratorUtil.generateValueConstraintsConstrainingAttribute(
                        row, header, filePath, fileSeparator, consAtt);
                for (ThingConstraint.Value<?> constraintValue : constraintValues) {
                    entityMatchStatement.constrain(GeneratorUtil.valueToHasConstraint(consAtt.getAttribute(), constraintValue));
                }
            }

            UnboundVariable insertUnboundVar = TypeQL.var("thing");
            ThingVariable.Thing insertStatement = null;
            for (Configuration.Definition.Attribute attributeToAppend : appendConfiguration.getInsert().getOwnerships()) {
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

    public boolean appendAttributeInsertStatementValid(TypeQLInsert insert) {
        if (insert == null) return false;
        if (!insert.toString().contains("isa " + appendConfiguration.getMatch().getType())) return false;
        for (Configuration.Definition.Attribute ownershipThingGetter : appendConfiguration.getMatch().getOwnerships()) {
            if (!insert.toString().contains("has " + ownershipThingGetter.getAttribute())) return false;
        }
        if (appendConfiguration.getInsert().getRequiredOwnerships() != null) {
            for (Configuration.Definition.Attribute attribute : appendConfiguration.getInsert().getRequiredOwnerships()) {
                if (!insert.toString().contains("has " + attribute.getAttribute())) return false;
            }
        }
        return true;
    }

    public char getFileSeparator() {
        return this.fileSeparator;
    }
}
