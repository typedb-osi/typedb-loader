package a_rewrite.generator;

import a_rewrite.config.Configuration;
import a_rewrite.util.GraknUtil;
import a_rewrite.util.Util;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
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
    //TODO: continue here!!!! --> need to validate that the relationGenerator creates appropriate statements for entity relations, then move on to test attribute relations and nested ones...

    @Test
    public void genericRelationTest() throws IOException {


        String dbName = "entity-generator-test";
        String sp = new File("src/test/resources/1.0.0/generic/schema.gql").getAbsolutePath();
        TypeDBClient client = GraknUtil.getClient("localhost:1729");
        GraknUtil.cleanAndDefineSchemaToDatabase(client, dbName, sp);

        String dcp = new File("src/test/resources/1.0.0/generic/dc.json").getAbsolutePath();
        Configuration dc = Util.initializeDataConfig(dcp);
        assert dc != null;
        ArrayList<String> relationKeys = new ArrayList<>(List.of("rel1"));
        TypeDBSession session = GraknUtil.getDataSession(client, dbName);
        for (String relationKey : relationKeys) {
            for (int idx = 0; idx < dc.getRelations().get(relationKey).getAttributes().length; idx++) {
                setRelationHasAttributeConceptType(relationKey, idx, dc, session);

            }
            for (int idx = 0; idx < dc.getRelations().get(relationKey).getPlayers().length; idx++) {
                setGetterAttributeConceptType(relationKey, idx, dc, session);
            }
        }
        session.close();
        client.close();

        String dp = new File("src/test/resources/1.0.0/generic/rel1.tsv").getAbsolutePath();
        RelationGenerator gen = new RelationGenerator(dp,
                dc.getRelations().get(relationKeys.get(0)),
                Objects.requireNonNullElseGet(dc.getRelations().get(relationKeys.get(0)).getSeparator(), () -> dc.getDefaultConfig().getSeparator()));
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
//        String dbName = "entity-generator-test";
//        String sp = new File("src/test/resources/1.0.0/phoneCalls/schema.gql").getAbsolutePath();
//        TypeDBClient client = GraknUtil.getClient("localhost:1729");
//        GraknUtil.cleanAndDefineSchemaToDatabase(client, dbName, sp);
//
//        String dp = new File("src/test/resources/1.0.0/phoneCalls/person.csv").getAbsolutePath();
//        String dcp = new File("src/test/resources/1.0.0/phoneCalls/dc.json").getAbsolutePath();
//        Configuration dc = Util.initializeDataConfig(dcp);
//        assert dc != null;
//        String entityKey = "person";
//        TypeDBSession session = GraknUtil.getDataSession(client, dbName);
//        for (int idx = 0; idx < dc.getEntities().get(entityKey).getAttributes().length; idx++) {
//            setEntityHasAttributeConceptType(entityKey, idx, dc, session);
//        }
//        EntityGenerator gen = new EntityGenerator(dp,
//                dc.getEntities().get(entityKey),
//                Objects.requireNonNullElseGet(dc.getEntities().get(entityKey).getSeparator(), () -> dc.getDefaultConfig().getSeparator()));
//
//        session.close();
//        client.close();
//
//        Iterator<String> iterator = Util.newBufferedReader(dp).lines().skip(1).iterator();
//
//        String tmp = "insert $e isa person, has first-name \"Melli\", has last-name \"Winchcum\", has phone-number \"+7 171 898 0853\", has city \"London\", has age 55;";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has first-name \"Celinda\", has last-name \"Bonick\", has phone-number \"+370 351 224 5176\", has city \"London\", has age 52;";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has first-name \"Chryste\", has last-name \"Lilywhite\", has phone-number \"+81 308 988 7153\", has city \"London\", has age 66;";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has first-name \"D'arcy\", has last-name \"Byfford\", has phone-number \"+54 398 559 0423\", has city \"London\", has age 19, has nick-name \"D\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has first-name \"Xylina\", has last-name \"D'Alesco\", has phone-number \"+7 690 597 4443\", has city \"Cambridge\", has age 51;";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has first-name \"Roldan\", has last-name \"Cometti\", has phone-number \"+263 498 495 0617\", has city \"Oxford\", has age 59, has nick-name \"Rolly\", has nick-name \"Rolli\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has first-name \"Cob\", has last-name \"Lafflin\", has phone-number \"+63 815 962 6097\", has city \"Cambridge\", has age 56;";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has first-name \"Olag\", has last-name \"Heakey\", has phone-number \"+81 746 154 2598\", has city \"London\", has age 45;";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has first-name \"Mandie\", has last-name \"Assender\", has phone-number \"+261 860 539 4754\", has city \"London\", has age 18;";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has first-name \"Elenore\", has last-name \"Stokey\", has phone-number \"+62 107 530 7500\", has city \"Oxford\", has age 35;";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+86 921 547 9004\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+48 894 777 5173\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+86 922 760 0418\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+33 614 339 0298\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+30 419 575 7546\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+7 414 625 3019\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+57 629 420 5680\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+351 515 605 7915\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+36 318 105 5629\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+63 808 497 1769\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+62 533 266 3426\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+351 272 414 6570\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+86 825 153 5518\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+86 202 257 8619\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+27 117 258 4149\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+48 697 447 6933\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+48 195 624 2025\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+1 254 875 4647\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+7 552 196 4096\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has phone-number \"+86 892 682 0628\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has first-name \"John\", has last-name \"Smith\", has phone-number \"+62 999 888 7777\", has city \"London\", has age 43, has nick-name \"Jack\", has nick-name \"J\";";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        tmp = "insert $e isa person, has first-name \"Jane\", has last-name \"Smith\", has phone-number \"+62 999 888 7778\", has city \"London\", has age 43;";
//        Assert.assertEquals(tmp, gen.generateInsertStatement(Util.parseCSV(iterator.next())).toString());
//
//        try {
//            gen.generateInsertStatement(Util.parseCSV(iterator.next()));
//        } catch (IndexOutOfBoundsException indexOutOfBoundsException) {
//            Assert.assertEquals("Index 0 out of bounds for length 0", indexOutOfBoundsException.getMessage());
//        }
//
//        try {
//            gen.generateInsertStatement(Util.parseCSV(iterator.next()));
//        } catch (IndexOutOfBoundsException indexOutOfBoundsException) {
//            Assert.assertEquals("Index 0 out of bounds for length 0", indexOutOfBoundsException.getMessage());
//        }
//
//        try {
//            gen.generateInsertStatement(Util.parseCSV(iterator.next()));
//        } catch (IndexOutOfBoundsException indexOutOfBoundsException) {
//            Assert.assertEquals("Index 0 out of bounds for length 0", indexOutOfBoundsException.getMessage());
//        }
    }

    private void setRelationHasAttributeConceptType(String relationKey, int attributeIndex, Configuration dc, TypeDBSession session) {
        dc.getRelations().get(relationKey).getAttributes()[attributeIndex].setConceptValueType(session.transaction(TypeDBTransaction.Type.READ));
    }

    private void setGetterAttributeConceptType(String relationKey, int playerIndex, Configuration dc, TypeDBSession session) {
        Configuration.Getter[] getters = dc.getRelations().get(relationKey).getPlayers()[playerIndex].getOwnershipGetters();
        for (Configuration.Getter ownershipGetter : getters){
            ownershipGetter.setConceptValueType(session.transaction(TypeDBTransaction.Type.READ));
        }
        Configuration.Getter attributeGetter = dc.getRelations().get(relationKey).getPlayers()[playerIndex].getAttributeGetter();
        if (attributeGetter != null) {
            attributeGetter.setConceptValueType(session.transaction(TypeDBTransaction.Type.READ));
        }
    }
}
