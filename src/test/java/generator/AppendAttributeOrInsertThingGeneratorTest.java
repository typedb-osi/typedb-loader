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

package generator;


import config.Configuration;
import util.TypeDBUtil;
import util.Util;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
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
        String sp = new File("src/test/resources/1.0.0/phoneCalls/schema.gql").getAbsolutePath();
        TypeDBClient client = TypeDBUtil.getClient("localhost:1729");
        TypeDBUtil.cleanAndDefineSchemaToDatabase(client, dbName, sp);

        String dcp = new File("src/test/resources/1.0.0/phoneCalls/dc.json").getAbsolutePath();
        Configuration dc = Util.initializeDataConfig(dcp);
        assert dc != null;
        ArrayList<String> appendOrInsertKeys = new ArrayList<>(List.of("append-or-insert-person"));
        TypeDBSession session = TypeDBUtil.getDataSession(client, dbName);
        for (String appendOrInsertkey : appendOrInsertKeys) {
            if (dc.getAppendAttributeOrInsertThing().get(appendOrInsertkey).getAttributes() != null) {
                Util.setConstrainingAttributeConceptType(dc.getAppendAttributeOrInsertThing().get(appendOrInsertkey).getAttributes(), session);
            }
            if (dc.getAppendAttributeOrInsertThing().get(appendOrInsertkey).getThingGetter() != null && dc.getAppendAttributeOrInsertThing().get(appendOrInsertkey).getThingGetter().getThingGetters() != null) {
                Util.setConstrainingAttributeConceptType(dc.getAppendAttributeOrInsertThing().get(appendOrInsertkey).getThingGetter().getThingGetters(), session);
            }
        }
        session.close();
        client.close();

        testPerson(dc, appendOrInsertKeys);
    }

    private void testPerson(Configuration dc, ArrayList<String> appendKeys) throws IOException {
        String dp = new File("src/test/resources/1.0.0/phoneCalls/person-append-or-insert.csv").getAbsolutePath();
        AppendAttributeOrInsertThingGenerator gen = new AppendAttributeOrInsertThingGenerator(dp,
                dc.getAppendAttributeOrInsertThing().get(appendKeys.get(0)),
                Objects.requireNonNullElseGet(dc.getAppendAttributeOrInsertThing().get(appendKeys.get(0)).getConfig().getSeparator(), () -> dc.getGlobalConfig().getSeparator()));
        Iterator<String> iterator = Util.newBufferedReader(dp).lines().skip(1).iterator();

        String[] row = Util.parseCSV(iterator.next());
        TypeQLInsert statement = gen.generateMatchInsertStatement(row);
        String tmp = "match $thing isa person, has phone-number \"+7 171 898 0853\";\n" +
                "insert $thing has first-name \"Melli\", has last-name \"Winchcum\", has city \"London\", has age 55, has nick-name \"Mel\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.appendAttributeInsertStatementValid(statement));
        statement = gen.generateThingInsertStatement(row);
        tmp = "insert $e isa person, has phone-number \"+7 171 898 0853\", has first-name \"Melli\", has last-name \"Winchcum\", has city \"London\", has age 55, has nick-name \"Mel\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.thingInsertStatementValid(statement));

        row = Util.parseCSV(iterator.next());
        statement = gen.generateMatchInsertStatement(row);
        tmp = "match $thing isa person;\n" +
                "insert $thing has first-name \"Sakura\", has city \"Fire Village\", has age 13;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.appendAttributeInsertStatementValid(statement));
        statement = gen.generateThingInsertStatement(row);
        tmp = "insert $e isa person, has first-name \"Sakura\", has city \"Fire Village\", has age 13;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.thingInsertStatementValid(statement));

        iterator.next();

        row = Util.parseCSV(iterator.next());
        statement = gen.generateMatchInsertStatement(row);
        tmp = "match $thing isa person, has phone-number \"+62 107 666 3334\";\n" +
                "insert $thing has first-name \"Sasuke\", has city \"Fire Village\", has age 13;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.appendAttributeInsertStatementValid(statement));
        statement = gen.generateThingInsertStatement(row);
        tmp = "insert $e isa person, has phone-number \"+62 107 666 3334\", has first-name \"Sasuke\", has city \"Fire Village\", has age 13;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.thingInsertStatementValid(statement));

        iterator.next();
        iterator.next();

        row = Util.parseCSV(iterator.next());
        statement = gen.generateMatchInsertStatement(row);
        tmp = "match $thing isa person, has phone-number \"+62 107 321 3333\";\n" +
                "insert $thing has first-name \"Missing\", has last-name \"Age\", has city \"notinsertcity\", has nick-name \"notinsertnickname\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.appendAttributeInsertStatementValid(statement));
        statement = gen.generateThingInsertStatement(row);
        tmp = "insert $e isa person, has phone-number \"+62 107 321 3333\", has first-name \"Missing\", has last-name \"Age\", has city \"notinsertcity\", has nick-name \"notinsertnickname\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.thingInsertStatementValid(statement));


    }
}
