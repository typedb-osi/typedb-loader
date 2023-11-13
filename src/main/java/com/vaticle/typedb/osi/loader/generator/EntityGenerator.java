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

import com.vaticle.typedb.driver.api.TypeDBTransaction;
import com.vaticle.typedb.driver.common.exception.TypeDBDriverException;
import com.vaticle.typedb.osi.loader.config.Configuration;
import com.vaticle.typedb.osi.loader.io.FileLogger;
import com.vaticle.typedb.osi.loader.util.GeneratorUtil;
import com.vaticle.typedb.osi.loader.util.TypeDBUtil;
import com.vaticle.typedb.osi.loader.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.statement.ThingStatement;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class EntityGenerator implements Generator {
    private static final Logger dataLogger = LogManager.getLogger("com.vaticle.typedb.osi.loader.error");
    private final String filePath;
    private final String[] header;
    private final Configuration.Generator.Entity entityConfiguration;
    private final char fileSeparator;

    public EntityGenerator(String filePath, Configuration.Generator.Entity entityConfiguration, char fileSeparator) throws IOException {
        this.filePath = filePath;
        this.header = Util.getFileHeader(filePath, fileSeparator);
        this.entityConfiguration = entityConfiguration;
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

        TypeQLInsert query = generateThingInsertStatement(row);
        if (valid(query)) {
            try {
                tx.query().insert(query);
            } catch (TypeDBDriverException typeDBDriverException) {
                FileLogger.getLogger().logUnavailable(fileName, originalRow);
                dataLogger.error("TypeDB Unavailable - Row in <" + filePath + "> not inserted - written to <" + fileNoExtension + "_unavailable.log" + ">");
            }
        } else {
            FileLogger.getLogger().logInvalid(fileName, originalRow);
            dataLogger.error("Invalid Row detected in <" + filePath + "> - written to <" + fileNoExtension + "_invalid.log" + "> - invalid Statement: <" + query.toString().replace("\n", " ") + ">");
        }
    }

    public TypeQLInsert generateThingInsertStatement(String[] row) {
        if (row.length > 0) {
            ThingStatement insertStatement = GeneratorUtil.generateBoundThingVar(entityConfiguration.getInsert().getEntity());

            GeneratorUtil.constrainThingWithHasAttributes(row, header, filePath, fileSeparator, insertStatement, entityConfiguration.getInsert().getOwnerships());

            return TypeQL.insert(insertStatement);
        } else {
            return TypeQL.insert(TypeQL.cVar("null").isa("null").has("null", "null"));
        }
    }

    public boolean valid(TypeQLInsert insert) {
        if (insert == null) return false;
        if (!insert.toString().contains("isa " + entityConfiguration.getInsert().getEntity())) return false;
        for (Configuration.Definition.Attribute attribute : entityConfiguration.getInsert().getRequiredOwnerships()) {
            if (!insert.toString().contains("has " + attribute.getAttribute())) return false;
        }
        return true;
    }

    public char getFileSeparator() {
        return this.fileSeparator;
    }
}
