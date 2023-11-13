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

package com.vaticle.typedb.osi.loader.cli;

import com.vaticle.typedb.driver.api.TypeDBDriver;
import com.vaticle.typedb.driver.api.TypeDBSession;
import com.vaticle.typedb.driver.api.TypeDBTransaction;
import com.vaticle.typedb.osi.loader.loader.TypeDBLoader;
import com.vaticle.typedb.osi.loader.util.TypeDBUtil;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TypeDBLoaderCLITest {

    @Test
    public void migrateTest() {
        String config = "src/test/resources/phoneCalls/config.json";
        String database = "typedb_loader_cli_test";
        String uri = "127.0.0.1:1729";
        String[] args = {
                "load",
                "-c", config,
                "-db", database,
                "-tdb", uri,
                "-cm"
        };

        LoadOptions options = LoadOptions.parse(args);
        TypeDBLoader loader = new TypeDBLoader(options);
        loader.load();

        assertEquals(options.dataConfigFilePath, config);
        assertEquals(options.databaseName, database);
        assertEquals(options.typedbURI, uri);
        assertTrue(options.cleanMigration);
        assertFalse(options.loadSchema);
    }

    @Test
    public void migrateTestContinue() {
        String config = "src/test/resources/phoneCalls/config.json";
        String db = "typedb_loader_cli_test";
        String uri = "127.0.0.1:1729";
        String[] cleanLoadArgs = {
                "load",
                "-c", config,
                "-db", db,
                "-tdb", uri,
                "-cm"
        };

        LoadOptions cleanLoadOptions = LoadOptions.parse(cleanLoadArgs);
        assertEquals(cleanLoadOptions.dataConfigFilePath, config);
        assertEquals(cleanLoadOptions.databaseName, db);
        assertEquals(cleanLoadOptions.typedbURI, uri);
        assertTrue(cleanLoadOptions.cleanMigration);
        assertFalse(cleanLoadOptions.loadSchema);

        // run import once
        new TypeDBLoader(cleanLoadOptions).load();
        // delete all the data
        clearData(uri, db);

        String[] continueArgs = {
                "load",
                "-c", config,
                "-db", db,
                "-tdb", uri
        };
        LoadOptions continueLoadOptions = LoadOptions.parse(continueArgs);
        assertEquals(continueLoadOptions.dataConfigFilePath, config);
        assertEquals(continueLoadOptions.databaseName, db);
        assertEquals(continueLoadOptions.typedbURI, uri);
        assertFalse(continueLoadOptions.cleanMigration);
        assertFalse(continueLoadOptions.loadSchema);

        // load all data and schema again
        new TypeDBLoader(continueLoadOptions).load();

        String[] continueLoadSchema = {
                "load",
                "-c", config,
                "-db", db,
                "-tdb", uri,
                "--loadSchema"
        };
        LoadOptions continueLoadSchemaOptions = LoadOptions.parse(continueLoadSchema);
        assertEquals(continueLoadSchemaOptions.dataConfigFilePath, config);
        assertEquals(continueLoadSchemaOptions.databaseName, db);
        assertEquals(continueLoadSchemaOptions.typedbURI, uri);
        assertFalse(continueLoadSchemaOptions.cleanMigration);
        assertTrue(continueLoadSchemaOptions.loadSchema);

        // load all data and schema again
        new TypeDBLoader(continueLoadSchemaOptions).load();
    }

    private void clearData(String uri, String db) {
        System.out.println("Cleaning all previous loaded data in: " + db);
        TypeDBDriver driver = TypeDBUtil.getCoreDriver(uri);
        try (TypeDBSession session = TypeDBUtil.getDataSession(driver, db)) {
            try (TypeDBTransaction txn = session.transaction(TypeDBTransaction.Type.WRITE)) {
                txn.query().delete(TypeQL.parseQuery("match $x isa thing; delete $x isa thing;").asDelete());
                txn.commit();
            }
        }
        driver.close();
    }
}
