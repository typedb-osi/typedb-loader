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

package com.vaticle.typedb.osi.generator;

import com.vaticle.typedb.osi.config.Configuration;
import com.vaticle.typedb.osi.util.QueryUtilTest;
import com.vaticle.typedb.osi.util.TypeDBUtil;
import com.vaticle.typedb.osi.util.Util;
import com.vaticle.typedb.client.api.TypeDBClient;
import com.vaticle.typedb.client.api.TypeDBSession;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class RelationGeneratorTest {

    @Test
    public void genericRelationTest() throws IOException {

        String dbName = "relation-generator-test";
        String sp = new File("src/test/resources/generic/schema.gql").getAbsolutePath();
        TypeDBClient client = TypeDBUtil.getClient("localhost:1729");
        TypeDBUtil.cleanAndDefineSchemaToDatabase(client, dbName, sp);

        String dcp = new File("src/test/resources/generic/config.json").getAbsolutePath();
        Configuration dc = Util.initializeConfig(dcp);
        assert dc != null;
        ArrayList<String> relationKeys = new ArrayList<>(List.of("rel1"));
        TypeDBSession session = TypeDBUtil.getDataSession(client, dbName);
        for (String relationKey : relationKeys) {
            if (dc.getRelations().get(relationKey).getInsert().getOwnerships() != null) {
                Configuration.Definition.Attribute[] hasAttributes = dc.getRelations().get(relationKey).getInsert().getOwnerships();
                Util.setConstrainingAttributeConceptType(hasAttributes, session);
            }
            for (int idx = 0; idx < dc.getRelations().get(relationKey).getInsert().getPlayers().length; idx++) {
                QueryUtilTest.setPlayerAttributeTypes(dc.getRelations().get(relationKey), idx, session);
            }
        }
        session.close();
        client.close();

        String dp = new File("src/test/resources/generic/rel1.tsv").getAbsolutePath();
        RelationGenerator gen = new RelationGenerator(dp,
                dc.getRelations().get(relationKeys.get(0)),
                Objects.requireNonNullElseGet(dc.getRelations().get(relationKeys.get(0)).getConfig().getSeparator(), () -> dc.getGlobalConfig().getSeparator()));
        Iterator<String> iterator = Util.newBufferedReader(dp).lines().skip(1).iterator();

        TypeQLInsert statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        String tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-1 \"att0\", has relAt-1 \"explosion0\", has relAt-2 \"opt0\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-1 \"att1\", has relAt-1 \"explosion1\", has relAt-1 \"explo1\", has relAt-2 \"opt1\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-1 \"att2\", has relAt-2 \"opt2\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-1 \"att3\", has relAt-2 \"opt3\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-1 \"att4\", has relAt-2 \"opt4\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-1 \"att5\", has relAt-2 \"opt5\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-1 \"att6\", has relAt-2 \"opt6\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-1 \"att7\", has relAt-2 \"opt7\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-1 \"att8\", has relAt-2 \"opt8\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-1 \"att9\", has relAt-2 \"opt9\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-1 \"att10\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1) isa rel1, has relAt-1 \"att19\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1) isa rel1, has relAt-1 \"att20\", has relAt-2 \"opt20\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1) isa rel1, has relAt-1 \"att21\", has relAt-1 \"explosion21\", has relAt-2 \"optional21\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-1 \"att22\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-2 \"opt25\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-1 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-two: $player-0, player-optional: $player-1) isa rel1, has relAt-1 \"att34\", has relAt-2 \"opt33\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\";\n" +
                "$player-1 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-optional: $player-1) isa rel1, has relAt-1 \"att37\", has relAt-2 \"opt36\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\", has entity1-id \"entity1id2\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-2 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1, player-optional: $player-2) isa rel1, has relAt-1 \"att39\", has relAt-2 \"opt39\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity1, has entity1-id \"entity1id1\", has entity1-id \"entity1id2\";\n" +
                "$player-1 isa entity2, has entity2-id \"entity2id1\";\n" +
                "insert $rel (player-one: $player-0, player-two: $player-1) isa rel1, has relAt-1 \"att40\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseTSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa entity2, has entity2-id \"entity2id1\";\n" +
                "$player-1 isa entity3, has entity3-id \"entity3id1\";\n" +
                "insert $rel (player-two: $player-0, player-optional: $player-1) isa rel1, has relAt-1 \"att41\", has relAt-2 \"opt41\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

    }

    @Test
    public void phoneCallsPersonTest() throws IOException {
        String dbName = "relation-generator-test";
        String sp = new File("src/test/resources/phoneCalls/schema.gql").getAbsolutePath();
        TypeDBClient client = TypeDBUtil.getClient("localhost:1729");
        TypeDBUtil.cleanAndDefineSchemaToDatabase(client, dbName, sp);

        String dcp = new File("src/test/resources/phoneCalls/config.json").getAbsolutePath();
        Configuration dc = Util.initializeConfig(dcp);
        assert dc != null;
        ArrayList<String> relationKeys = new ArrayList<>(List.of("contract", "call", "in-use", "communication-channel", "communication-channel-pm"));
        TypeDBSession session = TypeDBUtil.getDataSession(client, dbName);
        for (String relationKey : relationKeys) {
            if (dc.getRelations().get(relationKey).getInsert().getOwnerships() != null) {
                Configuration.Definition.Attribute[] hasAttributes = dc.getRelations().get(relationKey).getInsert().getOwnerships();
                Util.setConstrainingAttributeConceptType(hasAttributes, session);
            }
            for (int idx = 0; idx < dc.getRelations().get(relationKey).getInsert().getPlayers().length; idx++) {
                QueryUtilTest.setPlayerAttributeTypes(dc.getRelations().get(relationKey), idx, session);
                if (dc.getRelations().get(relationKey).getInsert().getPlayers()[idx].getMatch().getPlayers() != null) {
                    for (Configuration.Definition.Player player : dc.getRelations().get(relationKey).getInsert().getPlayers()[idx].getMatch().getPlayers()) {
                        if (player.getMatch().getOwnerships() != null) {
                            Util.setConstrainingAttributeConceptType(player.getMatch().getOwnerships(), session);
                        }
                    }
                }
            }
        }
        session.close();
        client.close();

        testContracts(dc, relationKeys);
        testCalls(dc, relationKeys);
        testInUse(dc, relationKeys);
        testCommunicationChannel(dc, relationKeys);
        testCommunicationChannelPM(dc, relationKeys);
    }

    private void testContracts(Configuration dc, ArrayList<String> relationKeys) throws IOException {
        String dp = new File("src/test/resources/phoneCalls/contract.csv.gz").getAbsolutePath();
        RelationGenerator gen = new RelationGenerator(dp,
                dc.getRelations().get(relationKeys.get(0)),
                Objects.requireNonNullElseGet(dc.getRelations().get(relationKeys.get(0)).getConfig().getSeparator(), () -> dc.getGlobalConfig().getSeparator()));
        Iterator<String> iterator = Util.newBufferedReader(dp).lines().skip(1).iterator();

        TypeQLInsert statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        String tmp = "match\n" +
                "$player-0 isa company, has name \"Telecom\";\n" +
                "$player-1 isa person, has phone-number \"+7 171 898 0853\";\n" +
                "insert $rel (provider: $player-0, customer: $player-1) isa contract;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        for (int i = 0; i < 7; i++) {
            iterator.next();
        }

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "insert $null isa null, has null \"null\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        iterator.next();

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match $player-0 isa person, has phone-number \"+261 860 539 4754\";\n" +
                "insert $rel (customer: $player-0) isa contract;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match $player-0 isa company, has name \"Telecom\";\n" +
                "insert $rel (provider: $player-0) isa contract;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        for (int i = 0; i < 3; i++) {
            iterator.next();
        }

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa company, has name \"Telecom\";\n" +
                "$player-1 isa person, has phone-number \"+62 107 530 7500\", has phone-number \"+261 860 539 4754\";\n" +
                "insert $rel (provider: $player-0, customer: $player-1) isa contract;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "insert $null isa null, has null \"null\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));
    }

    private void testCalls(Configuration dc, ArrayList<String> relationKeys) throws IOException {

        String dp = new File("src/test/resources/phoneCalls/call.csv").getAbsolutePath();
        RelationGenerator gen = new RelationGenerator(dp,
                dc.getRelations().get(relationKeys.get(1)),
                Objects.requireNonNullElseGet(dc.getRelations().get(relationKeys.get(1)).getConfig().getSeparator(), () -> dc.getGlobalConfig().getSeparator()));
        Iterator<String> iterator = Util.newBufferedReader(dp).lines().skip(1).iterator();

        TypeQLInsert statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        String tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+54 398 559 0423\";\n" +
                "$player-1 isa person, has phone-number \"+48 195 624 2025\";\n" +
                "insert $rel (caller: $player-0, callee: $player-1) isa call, has started-at 2018-09-16T22:24:19, has duration 122;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        for (int i = 0; i < 112; i++) {
            iterator.next();
        }

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+63 815 962 6097\";\n" +
                "$player-1 isa person, has phone-number \"+263 498 495 0617\";\n" +
                "insert $rel (caller: $player-0, callee: $player-1) isa call, has started-at 2018-09-19T23:16:49;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        for (int i = 0; i < 98; i++) {
            iterator.next();
        }

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+63 815 962 6097\";\n" +
                "$player-1 isa person, has phone-number \"+7 552 196 4096\";\n" +
                "insert $rel (caller: $player-0, callee: $player-1) isa call, has started-at 2018-09-23T01:14:56;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+63 815 962 6097\";\n" +
                "$player-1 isa person, has phone-number \"+7 552 196 4096\";\n" +
                "insert $rel (caller: $player-0, callee: $player-1) isa call, has started-at 2018-09-23T01:14:56;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+63 815 962 6097\";\n" +
                "$player-1 isa person, has phone-number \"+7 552 196 4096\";\n" +
                "insert $rel (caller: $player-0, callee: $player-1) isa call, has duration 53;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match $player-0 isa person, has phone-number \"+63 815 962 6097\";\n" +
                "insert $rel (caller: $player-0) isa call, has started-at 2018-09-23T01:14:56, has duration 53;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "insert $null isa null, has null \"null\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match $player-0 isa person, has phone-number \"+7 552 196 4096\";\n" +
                "insert $rel (callee: $player-0) isa call, has started-at 2018-09-23T01:14:56, has duration 53;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));
    }

    private void testInUse(Configuration dc, ArrayList<String> relationKeys) throws IOException {
        String dp = new File("src/test/resources/phoneCalls/in-use.csv").getAbsolutePath();
        RelationGenerator gen = new RelationGenerator(dp,
                dc.getRelations().get(relationKeys.get(2)),
                Objects.requireNonNullElseGet(dc.getRelations().get(relationKeys.get(2)).getConfig().getSeparator(), () -> dc.getGlobalConfig().getSeparator()));
        Iterator<String> iterator = Util.newBufferedReader(dp).lines().skip(1).iterator();

        TypeQLInsert statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        String tmp = "match\n" +
                "$player-0 \"yes\" isa is-in-use;\n" +
                "$player-1 \"+7 171 898 0853\" isa phone-number;\n" +
                "insert $rel (status: $player-0, account: $player-1) isa in-use;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        for (int i = 0; i < 4; i++) {
            iterator.next();
        }

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "insert $null isa null, has null \"null\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        for (int i = 0; i < 2; i++) {
            iterator.next();
        }

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match $player-0 \"+62 107 530 7500\" isa phone-number;\n" +
                "insert $rel (account: $player-0) isa in-use;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));


    }

    private void testCommunicationChannel(Configuration dc, ArrayList<String> relationKeys) throws IOException {
        String dp = new File("src/test/resources/phoneCalls/communication-channel.csv").getAbsolutePath();
        RelationGenerator gen = new RelationGenerator(dp,
                dc.getRelations().get(relationKeys.get(3)),
                Objects.requireNonNullElseGet(dc.getRelations().get(relationKeys.get(3)).getConfig().getSeparator(), () -> dc.getGlobalConfig().getSeparator()));
        Iterator<String> iterator = Util.newBufferedReader(dp).lines().skip(1).iterator();

        TypeQLInsert statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        String tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+54 398 559 0423\";\n" +
                "$player-1 isa person, has phone-number \"+48 195 624 2025\";\n" +
                "$player-2 isa call, has started-at 2018-09-16T22:24:19;\n" +
                "insert $rel (peer: $player-0, peer: $player-1, past-call: $player-2) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+263 498 495 0617\";\n" +
                "$player-1 isa person, has phone-number \"+33 614 339 0298\";\n" +
                "$player-2 isa call, has started-at 2018-09-11T22:10:34, has started-at 2018-09-12T22:10:34, has started-at 2018-09-13T22:10:34, has started-at 2018-09-14T22:10:34, has started-at 2018-09-15T22:10:34, has started-at 2018-09-16T22:10:34;\n" +
                "insert $rel (peer: $player-0, peer: $player-1, past-call: $player-2) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+263 498 495 0617\";\n" +
                "$player-1 isa person, has phone-number \"+33 614 339 0298\";\n" +
                "$player-2 isa call, has started-at 2018-09-11T22:10:34;\n" +
                "insert $rel (peer: $player-0, peer: $player-1, past-call: $player-2) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+370 351 224 5176\";\n" +
                "$player-1 isa person, has phone-number \"+62 533 266 3426\";\n" +
                "$player-2 isa call, has started-at 2018-09-15T12:12:59;\n" +
                "insert $rel (peer: $player-0, peer: $player-1, past-call: $player-2) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+62 533 266 3426\";\n" +
                "$player-1 isa call, has started-at 2018-09-15T12:12:59;\n" +
                "insert $rel (peer: $player-0, past-call: $player-1) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+370 351 224 5176\";\n" +
                "$player-1 isa call, has started-at 2018-09-15T12:12:59;\n" +
                "insert $rel (peer: $player-0, past-call: $player-1) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match $player-0 isa call, has started-at 2018-09-15T12:12:59;\n" +
                "insert $rel (past-call: $player-0) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+7 690 597 4443\";\n" +
                "$player-1 isa person, has phone-number \"+54 398 559 9999\";\n" +
                "insert $rel (peer: $player-0, peer: $player-1) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match $player-0 isa person, has phone-number \"+7 690 597 4443\";\n" +
                "insert $rel (peer: $player-0) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match $player-0 isa person, has phone-number \"+54 398 559 9999\";\n" +
                "insert $rel (peer: $player-0) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "insert $null isa null, has null \"null\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "insert $null isa null, has null \"null\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

    }

    private void testCommunicationChannelPM(Configuration dc, ArrayList<String> relationKeys) throws IOException {
        String dp = new File("src/test/resources/phoneCalls/communication-channel-pm.csv").getAbsolutePath();
        RelationGenerator gen = new RelationGenerator(dp,
                dc.getRelations().get(relationKeys.get(4)),
                Objects.requireNonNullElseGet(dc.getRelations().get(relationKeys.get(4)).getConfig().getSeparator(), () -> dc.getGlobalConfig().getSeparator()));
        Iterator<String> iterator = Util.newBufferedReader(dp).lines().skip(1).iterator();

        TypeQLInsert statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        String tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+81 308 988 7153\";\n" +
                "$player-1 isa person, has phone-number \"+351 515 605 7915\";\n" +
                "$player-2-0 isa person, has phone-number \"+81 308 988 7153\";\n" +
                "$player-2-1 isa person, has phone-number \"+351 515 605 7915\";\n" +
                "$player-2 (caller: $player-2-0, callee: $player-2-1) isa call;\n" +
                "insert $rel (peer: $player-0, peer: $player-1, past-call: $player-2) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+7 171 898 0853\";\n" +
                "$player-1 isa person, has phone-number \"+57 629 420 5680\";\n" +
                "$player-2-0 isa person, has phone-number \"+7 171 898 0853\";\n" +
                "$player-2-1 isa person, has phone-number \"+57 629 420 5680\";\n" +
                "$player-2 (caller: $player-2-0, callee: $player-2-1) isa call;\n" +
                "insert $rel (peer: $player-0, peer: $player-1, past-call: $player-2) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertTrue(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+261 860 539 4754\";\n" +
                "$player-1-0 isa person, has phone-number \"+261 860 539 4754\";\n" +
                "$player-1 (caller: $player-1-0, callee: $player-1-1) isa call;\n" +
                "insert $rel (peer: $player-0, past-call: $player-1) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match $player-0 (caller: $player-0-0, callee: $player-0-1) isa call;\n" +
                "insert $rel (past-call: $player-0) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match $player-0 (caller: $player-0-0, callee: $player-0-1) isa call;\n" +
                "insert $rel (past-call: $player-0) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "insert $null isa null, has null \"null\";";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

        statement = gen.generateMatchInsertStatement(Util.parseCSV(iterator.next()));
        tmp = "match\n" +
                "$player-0 isa person, has phone-number \"+261 860 539 4754\";\n" +
                "$player-1-1 isa person, has phone-number \"+261 860 539 4754\";\n" +
                "$player-1 (caller: $player-1-0, callee: $player-1-1) isa call;\n" +
                "insert $rel (peer: $player-0, past-call: $player-1) isa communication-channel;";
        Assert.assertEquals(tmp, statement.toString());
        Assert.assertFalse(gen.relationInsertStatementValid(statement));

    }
}
