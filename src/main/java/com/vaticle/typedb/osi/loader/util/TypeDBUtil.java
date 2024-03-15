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

import com.vaticle.typedb.driver.TypeDB;
import com.vaticle.typedb.driver.api.TypeDBDriver;
import com.vaticle.typedb.driver.api.TypeDBCredential;
import com.vaticle.typedb.driver.api.TypeDBSession;
import com.vaticle.typedb.driver.api.TypeDBTransaction;
import com.vaticle.typedb.driver.api.answer.ConceptMap;
import com.vaticle.typedb.driver.api.concept.Concept;
import com.vaticle.typedb.osi.loader.cli.LoadOptions;
import com.vaticle.typedb.osi.loader.io.FileLogger;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.statement.Statement;
import com.vaticle.typeql.lang.pattern.statement.ThingStatement;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.vaticle.typedb.osi.loader.util.Util.loadSchemaFromFile;

public class TypeDBUtil {

    public static TypeDBDriver getDriver(LoadOptions options) {
        if (options.typedbClusterURI != null) {
            TypeDBCredential credential;
            if (options.tlsEnabled) {
                if (options.tlsRootCAPath == null)
                    throw new RuntimeException("When TLS is enabled a Root CA path must be provided");
                credential = new TypeDBCredential(options.username, options.password, Path.of(options.tlsRootCAPath));
            } else {
                credential = new TypeDBCredential(options.username, options.password, false);
            }
            return TypeDB.cloudDriver(options.typedbClusterURI, credential);
        } else {
            return TypeDB.coreDriver(options.typedbURI);
        }
    }

    public static TypeDBDriver getCoreDriver(String typedbURI) {
        return TypeDB.coreDriver(typedbURI);
    }

    public static TypeDBSession getDataSession(TypeDBDriver driver, String databaseName) {
        return driver.session(databaseName, TypeDBSession.Type.DATA);
    }

    public static TypeDBSession getSchemaSession(TypeDBDriver driver, String databaseName) {
        return driver.session(databaseName, TypeDBSession.Type.SCHEMA);
    }

    private static void createDatabase(TypeDBDriver driver, String databaseName) {
        driver.databases().create(databaseName);
    }

    private static void deleteDatabaseIfExists(TypeDBDriver driver, String databaseName) {
        if (driver.databases().contains(databaseName)) {
            driver.databases().get(databaseName).delete();
        }
    }

    public static void cleanAndDefineSchemaToDatabase(TypeDBDriver driver, String databaseName, String schemaPath) {
        deleteDatabaseIfExists(driver, databaseName);
        createDatabase(driver, databaseName);
        loadAndDefineSchema(driver, databaseName, schemaPath);
    }

    private static void defineToTypeDB(TypeDBDriver driver, String databaseName, String schemaAsString) {
        TypeDBSession schemaSession = getSchemaSession(driver, databaseName);
        TypeQLDefine q = TypeQL.parseQuery(schemaAsString);

        try (TypeDBTransaction writeTransaction = schemaSession.transaction(TypeDBTransaction.Type.WRITE)) {
            writeTransaction.query().define(q);
            writeTransaction.commit();
            writeTransaction.close();
            schemaSession.close();
        }

        Util.info("Defined schema to database <" + databaseName + ">");
    }

    public static void loadAndDefineSchema(TypeDBDriver driver, String databaseName, String schemaPath) {
        String schema = loadSchemaFromFile(schemaPath);
        defineToTypeDB(driver, databaseName, schema);
    }

    public static Iterator<ConceptMap> executeMatch(TypeDBTransaction tx, TypeQLInsert query) {
        if (!query.match().isPresent()) throw new RuntimeException("Expected TypeQL 'match' to be present");
        return tx.query().get(query.match().get().get()).iterator();
    }

    public static void safeInsert(TypeDBTransaction tx, TypeQLInsert query, Iterator<ConceptMap> matches, boolean allowMultiInsert, String filePath, String row, Logger dataLogger) {
        assert query.match().isPresent();
        String fileName = FilenameUtils.getName(filePath);
        ConceptMap answer = matches.next();
        if (!allowMultiInsert && matches.hasNext()) {
            FileLogger.getLogger().logTooManyMatches(fileName, row);
            dataLogger.error("Match-insert skipped - File <" + filePath + "> row <" + row + "> generates query <" + query + "> which matched more than 1 answer.");
        } else {
            tx.query().insert(TypeDBUtil.replaceMatchWithAnswer(query, answer));
            while (matches.hasNext()) {
                answer = matches.next();
                tx.query().insert(TypeDBUtil.replaceMatchWithAnswer(query, answer));
            }
        }
    }

    public static TypeQLInsert replaceMatchWithAnswer(TypeQLInsert query, ConceptMap ans) {
        assert query.match().isPresent();
        List<ThingStatement<?>> insertVars = query.asInsert().statements();
        List<Statement> matchVars = new ArrayList<>();
        ans.variables().forEach((var) -> {
            Concept concept = ans.get(var);
            if (concept.isThing()) matchVars.add(TypeQL.cVar(var).iid(concept.asThing().getIID()));
            else if (concept.asType().getLabel().scope().isPresent()) {
                matchVars.add(TypeQL.cVar(var).type(concept.asType().getLabel().scope().get(), concept.asType().getLabel().name()));
            } else matchVars.add(TypeQL.cVar(var).type(concept.asType().getLabel().name()));
        });
        return TypeQL.match(matchVars).insert(insertVars);
    }
}
