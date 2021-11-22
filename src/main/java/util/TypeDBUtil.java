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

package util;

import com.vaticle.typedb.client.TypeDB;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static util.Util.loadSchemaFromFile;

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

    private static void defineToGrakn(TypeDBClient client, String databaseName, String schemaAsString) {
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
        defineToGrakn(client, databaseName, schema);
    }

}
