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

import com.vaticle.typedb.driver.api.TypeDBDriver;
import com.vaticle.typedb.driver.api.TypeDBSession;
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

public class AppendAttributeGeneratorTest {

    @Test
    public void phoneCallsPersonTest() throws IOException {
        String dbName = "append-attribute-generator-test";
        String sp = new File("src/test/resources/phoneCalls/schema.gql").getAbsolutePath();
        TypeDBDriver driver = TypeDBUtil.getCoreDriver("localhost:1729");
        TypeDBUtil.cleanAndDefineSchemaToDatabase(driver, dbName, sp);

        String dcp = new File("src/test/resources/phoneCalls/config.json").getAbsolutePath();
        Configuration dc = Util.initializeConfig(dcp);
        assert dc != null;
        ArrayList<String> appendKeys = new ArrayList<>(List.of("append-twitter", "append-fakebook", "append-call-rating"));
        TypeDBSession session = TypeDBUtil.getDataSession(driver, dbName);
        for (String appendkey : appendKeys) {
            Configuration.Definition.Attribute[] hasAttributes = dc.getAppendAttribute().get(appendkey).getInsert().getOwnerships();
            if (hasAttributes != null) {
                Util.setConstrainingAttributeConceptType(hasAttributes, session);
            }
            if (dc.getAppendAttribute().get(appendkey).getMatch() != null && dc.getAppendAttribute().get(appendkey).getMatch().getOwnerships() != null) {
                Configuration.Definition.Attribute[] thingGetterAttributes = dc.getAppendAttribute().get(appendkey).getMatch().getOwnerships();
                Util.setConstrainingAttributeConceptType(thingGetterAttributes, session);
            }
        }
        session.close();
        driver.close();

        testTwitter(dc, appendKeys);
        testFakebook(dc, appendKeys);
        testCallAppend(dc, appendKeys);
    }

    private void testTwitter(Configuration dc, ArrayList<String> appendKeys) throws IOException {
        String dp = new File("src/test/resources/phoneCalls/append-twitter-nickname.csv").getAbsolutePath();
        AppendAttributeGenerator gen = new AppendAttributeGenerator(dp,
                dc.getAppendAttribute().get(appendKeys.get(0)),
                Objects.requireNonNullElseGet(dc.getAppendAttribute().get(appendKeys.get(0)).getConfig().getSeparator(), () -> dc.getGlobalConfig().getSeparator()));
        Iterator<String> iterator = Util.newBufferedReader(dp).lines().skip(1).iterator();

        TypeQLInsert statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        TypeQLInsert tmp = TypeQL.parseQuery("match $thing isa person, has phone-number \"+7 171 898 0853\";\n" +
                "insert $thing has twitter-username \"@jojo\", has nick-name \"another\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.appendAttributeInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("match $thing isa person, has phone-number \"+263 498 495 0617\";\n" +
                "insert $thing has twitter-username \"@hui\", has twitter-username \"@bui\", has nick-name \"yetanoter\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.appendAttributeInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("match $thing isa person, has phone-number \"+370 351 224 5176\";\n" +
                "insert $thing has twitter-username \"@lalulix\", has nick-name \"one more\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.appendAttributeInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("match $thing isa person, has phone-number \"+81 308 988 7153\";\n" +
                "insert $thing has twitter-username \"@go34\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.appendAttributeInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("match $thing isa person, has phone-number \"+54 398 559 0423\";\n" +
                "insert $thing has twitter-username \"@hadaaa\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.appendAttributeInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("match $thing isa person, has phone-number \"+7 690 597 4443\";\n" +
                "insert $thing has nick-name \"not inserted\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertFalse(gen.appendAttributeInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("match $thing isa person, has phone-number \"+63 815 962 6097\";\n" +
                "insert $thing has twitter-username \"@kuka\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.appendAttributeInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("insert $null isa null, has null \"null\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertFalse(gen.appendAttributeInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("match $thing isa person;\n" +
                "insert $thing has twitter-username \"@notinserted\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertFalse(gen.appendAttributeInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("insert $null isa null, has null \"null\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertFalse(gen.appendAttributeInsertStatementValid(statement));

    }

    private void testFakebook(Configuration dc, ArrayList<String> appendKeys) throws IOException {
        String dp = new File("src/test/resources/phoneCalls/append-fb-preprocessed.csv").getAbsolutePath();
        AppendAttributeGenerator gen = new AppendAttributeGenerator(dp,
                dc.getAppendAttribute().get(appendKeys.get(1)),
                Objects.requireNonNullElseGet(dc.getAppendAttribute().get(appendKeys.get(1)).getConfig().getSeparator(), () -> dc.getGlobalConfig().getSeparator()));
        Iterator<String> iterator = Util.newBufferedReader(dp).lines().skip(1).iterator();

        TypeQLInsert statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        TypeQLInsert tmp = TypeQL.parseQuery("match $thing isa person, has phone-number \"+36 318 105 5629\";\n" +
                "insert $thing has fakebook-link \"fakebook.com/personOne\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.appendAttributeInsertStatementValid(statement));

        iterator.next();
        iterator.next();

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("match $thing isa person, has phone-number \"+62 533 266 3426\";\n" +
                "insert $thing has fakebook-link \"insertedWithoutAppliedRegex\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.appendAttributeInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("match $thing isa person;\n" +
                "insert $thing has fakebook-link \"@notinserted\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertFalse(gen.appendAttributeInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("insert $null isa null, has null \"null\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertFalse(gen.appendAttributeInsertStatementValid(statement));
    }

    private void testCallAppend(Configuration dc, ArrayList<String> appendKeys) throws IOException {
        String dp = new File("src/test/resources/phoneCalls/append-call-rating.csv").getAbsolutePath();
        AppendAttributeGenerator gen = new AppendAttributeGenerator(dp,
                dc.getAppendAttribute().get(appendKeys.get(2)),
                Objects.requireNonNullElseGet(dc.getAppendAttribute().get(appendKeys.get(2)).getConfig().getSeparator(), () -> dc.getGlobalConfig().getSeparator()));
        Iterator<String> iterator = Util.newBufferedReader(dp).lines().skip(1).iterator();

        TypeQLInsert statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        TypeQLInsert tmp = TypeQL.parseQuery("match $thing isa call, has started-at 2018-09-19T01:00:38;\n" +
                "insert $thing has call-rating 5;").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertTrue(gen.appendAttributeInsertStatementValid(statement));

        iterator.next();
        iterator.next();
        iterator.next();
        iterator.next();

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("insert $null isa null, has null \"null\";").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertFalse(gen.appendAttributeInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = TypeQL.parseQuery("match $thing isa call;\n" +
                "insert $thing has call-rating 4;").asInsert();
        Assert.assertEquals(tmp, statement);
        Assert.assertFalse(gen.appendAttributeInsertStatementValid(statement));
    }
}
