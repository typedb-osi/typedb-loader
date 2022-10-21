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

import com.vaticle.typedb.client.TypeDB;
import com.vaticle.typedb.client.api.TypeDBClient;
import com.vaticle.typedb.client.api.TypeDBSession;
import com.vaticle.typedb.client.api.TypeDBTransaction;
import com.vaticle.typedb.client.api.answer.ConceptMap;
import com.vaticle.typedb.osi.loader.io.FileLogger;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.variable.BoundVariable;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.vaticle.typedb.osi.loader.util.Util.loadSchemaFromFile;

public class TypeDBUtil {

    public static TypeDBClient getClient(String graknURI) {
        return TypeDB.coreClient(graknURI);
    }

    public static TypeDBClient getClient(String graknURI, int parallelization) {
        return TypeDB.coreClient(graknURI, parallelization);
    }

    public static TypeDBSession getDataSession(TypeDBClient client, String databaseName) {
        return client.session(databaseName, TypeDBSession.Type.DATA);
    }

    public static TypeDBSession getSchemaSession(TypeDBClient client, String databaseName) {
        return client.session(databaseName, TypeDBSession.Type.SCHEMA);
    }

    private static void createDatabase(TypeDBClient client, String databaseName) {
        client.databases().create(databaseName);
    }

    private static void deleteDatabaseIfExists(TypeDBClient client, String databaseName) {
        if (client.databases().contains(databaseName)) {
            client.databases().get(databaseName).delete();
        }
    }

    public static void cleanAndDefineSchemaToDatabase(TypeDBClient client, String databaseName, String schemaPath) {
        deleteDatabaseIfExists(client, databaseName);
        createDatabase(client, databaseName);
        loadAndDefineSchema(client, databaseName, schemaPath);
    }

    private static void defineToTypeDB(TypeDBClient client, String databaseName, String schemaAsString) {
        TypeDBSession schemaSession = getSchemaSession(client, databaseName);
        TypeQLDefine q = TypeQL.parseQuery(schemaAsString);

        try (TypeDBTransaction writeTransaction = schemaSession.transaction(TypeDBTransaction.Type.WRITE)) {
            writeTransaction.query().define(q);
            writeTransaction.commit();
            writeTransaction.close();
            schemaSession.close();
        }

        Util.info("Defined schema to database <" + databaseName + ">");
    }

    public static void loadAndDefineSchema(TypeDBClient client, String databaseName, String schemaPath) {
        String schema = loadSchemaFromFile(schemaPath);
        defineToTypeDB(client, databaseName, schema);
    }

    public static Iterator<ConceptMap> executeMatch(TypeDBTransaction tx, TypeQLInsert query) {
        if (!query.match().isPresent()) throw new RuntimeException("Expected TypeQL 'match' to be present");
        return tx.query().match(query.match().get()).iterator();
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
        List<ThingVariable<?>> insertVars = query.asInsert().variables();
        List<BoundVariable> matchVars = new ArrayList<>();
        ans.map().forEach((var, concept) -> {
            if (concept.isThing()) matchVars.add(TypeQL.var(var).iid(concept.asThing().getIID()));
            else if (concept.asType().getLabel().scope().isPresent()) {
                matchVars.add(TypeQL.var(var).type(concept.asType().getLabel().scope().get(), concept.asType().getLabel().name()));
            } else matchVars.add(TypeQL.var(var).type(concept.asType().getLabel().name()));
        });
        return TypeQL.match(matchVars).insert(insertVars);
    }
}
