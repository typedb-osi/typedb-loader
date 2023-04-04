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


import com.vaticle.typedb.client.api.TypeDBClient;
import com.vaticle.typedb.client.api.TypeDBSession;
import com.vaticle.typedb.osi.loader.config.Configuration;
import com.vaticle.typedb.osi.loader.util.TypeDBUtil;
import com.vaticle.typedb.osi.loader.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class AppendAttributeOrInsertThingGeneratorTest {

    @Test
    public void phoneCallsPersonTest() throws IOException {
        String dbName = "append-or-insert-generator-test";
        String sp = new File("src/test/resources/phoneCalls/schema.gql").getAbsolutePath();
        TypeDBClient client = TypeDBUtil.getCoreClient("localhost:1729");
        TypeDBUtil.cleanAndDefineSchemaToDatabase(client, dbName, sp);

        String dcp = new File("src/test/resources/phoneCalls/config.json").getAbsolutePath();
        Configuration dc = Util.initializeConfig(dcp);
        assert dc != null;
        ArrayList<String> appendOrInsertKeys = new ArrayList<>(List.of("append-or-insert-person"));
        TypeDBSession session = TypeDBUtil.getDataSession(client, dbName);
        for (String appendOrInsertkey : appendOrInsertKeys) {
            if (dc.getAppendAttributeOrInsertThing().get(appendOrInsertkey).getInsert().getOwnerships() != null) {
                Util.setConstrainingAttributeConceptType(dc.getAppendAttributeOrInsertThing().get(appendOrInsertkey).getInsert().getOwnerships(), session);
            }
            if (dc.getAppendAttributeOrInsertThing().get(appendOrInsertkey).getMatch() != null && dc.getAppendAttributeOrInsertThing().get(appendOrInsertkey).getMatch().getOwnerships() != null) {
                Util.setConstrainingAttributeConceptType(dc.getAppendAttributeOrInsertThing().get(appendOrInsertkey).getMatch().getOwnerships(), session);
            }
        }
        session.close();
        client.close();

        testPerson(dc, appendOrInsertKeys);
    }

    private void testPerson(Configuration dc, ArrayList<String> appendKeys) throws IOException {
        String dp = new File("src/test/resources/phoneCalls/person-append-or-insert.csv").getAbsolutePath();
        AppendAttributeOrInsertThingGenerator gen = new AppendAttributeOrInsertThingGenerator(dp,
                dc.getAppendAttributeOrInsertThing().get(appendKeys.get(0)),
                Objects.requireNonNullElseGet(dc.getAppendAttributeOrInsertThing().get(appendKeys.get(0)).getConfig().getSeparator(), () -> dc.getGlobalConfig().getSeparator()));
        Iterator<String> iterator = Util.newBufferedReader(dp).lines().skip(1).iterator();

        String[] row = Util.parseCSV(iterator.next());
        TypeQLInsert statement = gen.generateMatchInsertStatement(row);
        TypeQLInsert tmp = TypeQL.parseQuery("match $thing isa person, has phone-number \"+7 171 898 0853\";\n" +
                "insert $thing has first-name \"Melli\", has last-name \"Winchcum\", has city \"London\", has age 55, has nick-name \"Mel\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.appendAttributeInsertStatementValid(statement));
        statement = gen.generateThingInsertStatement(row);
        tmp = TypeQL.parseQuery("insert $e isa person, has phone-number \"+7 171 898 0853\", has first-name \"Melli\", has last-name \"Winchcum\", has city \"London\", has age 55, has nick-name \"Mel\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.thingInsertStatementValid(statement));

        row = Util.parseCSV(iterator.next());
        statement = gen.generateMatchInsertStatement(row);
        tmp = TypeQL.parseQuery("match $thing isa person;\n" +
                "insert $thing has first-name \"Sakura\", has city \"Fire Village\", has age 13;").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertFalse(gen.appendAttributeInsertStatementValid(statement));
        statement = gen.generateThingInsertStatement(row);
        tmp = TypeQL.parseQuery("insert $e isa person, has first-name \"Sakura\", has city \"Fire Village\", has age 13;").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.thingInsertStatementValid(statement));

        iterator.next();

        row = Util.parseCSV(iterator.next());
        statement = gen.generateMatchInsertStatement(row);
        tmp = TypeQL.parseQuery("match $thing isa person, has phone-number \"+62 107 666 3334\";\n" +
                "insert $thing has first-name \"Sasuke\", has city \"Fire Village\", has age 13;").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.appendAttributeInsertStatementValid(statement));
        statement = gen.generateThingInsertStatement(row);
        tmp = TypeQL.parseQuery("insert $e isa person, has phone-number \"+62 107 666 3334\", has first-name \"Sasuke\", has city \"Fire Village\", has age 13;").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.thingInsertStatementValid(statement));

        iterator.next();
        iterator.next();

        row = Util.parseCSV(iterator.next());
        statement = gen.generateMatchInsertStatement(row);
        tmp = TypeQL.parseQuery("match $thing isa person, has phone-number \"+62 107 321 3333\";\n" +
                "insert $thing has first-name \"Missing\", has last-name \"Age\", has city \"notinsertcity\", has nick-name \"notinsertnickname\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertFalse(gen.appendAttributeInsertStatementValid(statement));
        statement = gen.generateThingInsertStatement(row);
        tmp = TypeQL.parseQuery("insert $e isa person, has phone-number \"+62 107 321 3333\", has first-name \"Missing\", has last-name \"Age\", has city \"notinsertcity\", has nick-name \"notinsertnickname\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertFalse(gen.thingInsertStatementValid(statement));


    }
}
