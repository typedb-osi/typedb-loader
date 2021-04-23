package insert;

import processor.ProcessorStatement;
import grakn.client.api.GraknClient;
import grakn.client.api.GraknSession;
import grakn.client.api.GraknTransaction;
import graql.lang.Graql;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import org.junit.Assert;
import org.junit.Test;
import util.Util;

import java.util.ArrayList;

import static graql.lang.Graql.var;

public class GraknInserterTest {

    GraknInserter gi;
    GraknInserter pcgi;
    String databaseName = "grakn_inserter_test";
    String schemaPath;

    public GraknInserterTest() {
        this.schemaPath = Util.getAbsPath("src/test/resources/genericTests/schema-test.gql");
        this.gi = new GraknInserter("localhost", "1729", schemaPath, databaseName);
        this.pcgi = new GraknInserter("localhost", "1729", "src/test/resources/phone-calls/schema-updated.gql", databaseName);
    }

    @Test
    public void reloadKeyspaceTest() {
        GraknClient client = gi.getClient();

        gi.cleanAndDefineSchemaToDatabase(client);
        Assert.assertTrue(client.databases().contains(databaseName));

        //ensure Keyspace contains schema
        GraknSession dataSession = gi.getDataSession(client);
        GraknTransaction read = dataSession.transaction(GraknTransaction.Type.READ);
        GraqlMatch.Limited mq = Graql.match(var("e").sub("entity")).get("e").limit(100);
        Assert.assertEquals(4, read.query().match(mq).count());
        read.close();

        read = dataSession.transaction(GraknTransaction.Type.READ);
        mq = Graql.match(var("e").type("entity1")).get("e").limit(100);
        Assert.assertEquals(1, read.query().match(mq).count());
        read.close();

        dataSession.close();
        client.close();
    }

    @Test
    public void loadSchemaFromFileTest() {
        String schema = Util.loadSchemaFromFile(schemaPath);

        Assert.assertTrue("schema test positive", schema.contains("entity1 sub entity"));
        Assert.assertFalse("schema test negative", schema.contains("entity99 sub entity"));
    }

    @Test
    public void insertToGraknTest() {

        GraknClient client = gi.getClient();
        gi.cleanAndDefineSchemaToDatabase(client);
        GraknSession dataSession = gi.getDataSession(client);

        //perform data entry
        GraknTransaction write = dataSession.transaction(GraknTransaction.Type.WRITE);
        GraqlInsert insertQuery = Graql.insert(var("e").isa("entity1").has("entity1-id", "ide1"));
        write.query().insert(insertQuery);
        write.commit();
        write.close();

        //ensure graph contains our insert
        GraknTransaction read = dataSession.transaction(GraknTransaction.Type.READ);
        GraqlMatch.Limited getQuery = Graql.match(var("e").isa("entity1").has("entity1-id", "ide1")).get("e").limit(100);
        Assert.assertEquals(1, read.query().match(getQuery).count());
        read.close();

        read = dataSession.transaction(GraknTransaction.Type.READ);
        getQuery = Graql.match(var("e").isa("entity1").has("entity1-id", "ide2")).limit(100);
        Assert.assertEquals(0, read.query().match(getQuery).count());
        read.close();

        //another test for our insert
        read = dataSession.transaction(GraknTransaction.Type.READ);
        getQuery = Graql.match(var("e").isa("entity1").has("entity1-id", "ide1")).get("e").limit(100);
        read.query().match(getQuery).forEach(answers -> answers.concepts().forEach(entry -> Assert.assertTrue(entry.isEntity())));
        read.close();

        //clean up:
        GraknTransaction delete = dataSession.transaction(GraknTransaction.Type.WRITE);
        GraqlDelete delQuery = Graql.match(
                var("e").isa("entity1").has("entity1-id", "ide1")
        ).delete(var("e").isa("entity1"));
        delete.query().delete(delQuery);
        delete.commit();
        delete.close();

        dataSession.close();
        client.close();
    }

    @Test
    public void threadedInsertGraknTest() throws InterruptedException {

        GraknClient client = pcgi.getClient();
        pcgi.cleanAndDefineSchemaToDatabase(client);
        GraknSession dataSession = pcgi.getDataSession(client);

        ArrayList<ThingVariable<?>> singleThreadStatements = new ArrayList<>();
        singleThreadStatements.add(null);
        singleThreadStatements.add(var("person").isa("person").has("first-name", "first-first-name").has("phone-number", "+47 1234 1234 1"));
        singleThreadStatements.add(null);
        singleThreadStatements.add(var("person").isa("person").has("first-name", "second-first-name").has("phone-number", "+47 1234 1234 2"));
        singleThreadStatements.add(null);
        pcgi.insertThreadedInserting(singleThreadStatements, dataSession, 1, 1);

        ArrayList<ThingVariable<?>> multiThreadStatements = new ArrayList<>();
        multiThreadStatements.add(null);
        multiThreadStatements.add(var("person").isa("person").has("first-name", "first-first-name-mt").has("phone-number", "+47 1234 1234 3"));
        multiThreadStatements.add(null);
        multiThreadStatements.add(var("person").isa("person").has("first-name", "second-first-name-mt").has("phone-number", "+47 1234 1234 4"));
        multiThreadStatements.add(null);
        pcgi.insertThreadedInserting(multiThreadStatements, dataSession, 4, 1);

        pcgi.insertThreadedInserting(new ArrayList<>(), dataSession, 1, 1);
        pcgi.insertThreadedInserting(new ArrayList<>(), dataSession, 4, 1);

        GraknTransaction read = dataSession.transaction(GraknTransaction.Type.READ);

        GraqlMatch getQuery = Graql.match(var("p").isa("person")).get("p").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());

        getQuery = Graql.match(var("p").isa("person").has("first-name", "first-first-name")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("p").isa("person").has("first-name", "second-first-name")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("p").isa("person").has("first-name", "first-first-name-mt")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("p").isa("person").has("first-name", "first-first-name-mt")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("p").isa("person").has("phone-number", "+47 1234 1234 1")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("p").isa("person").has("phone-number", "+47 1234 1234 2")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("p").isa("person").has("phone-number", "+47 1234 1234 3")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("p").isa("person").has("phone-number", "+47 1234 1234 4")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        read.close();
        dataSession.close();
        client.close();
    }

    @Test
    public void threadedMatchInsertGraknTest() throws InterruptedException {

        threadedInsertGraknTest();

        GraknClient client = pcgi.getClient();
        GraknSession dataSession = pcgi.getDataSession(client);

        // single-thread inserting
        ProcessorStatement singleThreadStatements = new ProcessorStatement();

        singleThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(null, null));

        ArrayList<ThingVariable<?>> curMatch = new ArrayList<>();
        ThingVariable<?> curInsert;
        curMatch.add(var("p1").isa("person").has("phone-number", "+47 1234 1234 1"));
        curMatch.add(var("p2").isa("person").has("phone-number", "+47 1234 1234 2"));
        curInsert = var("rel").rel("caller", "p1").rel("callee", "p2").isa("call");
        singleThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(curMatch, curInsert));

        singleThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(null, null));

        ArrayList<ThingVariable<?>> curMatchB = new ArrayList<>();
        ThingVariable<?> curInsertB;
        curMatchB.add(var("p1").isa("person").has("phone-number", "+47 1234 1234 3"));
        curMatchB.add(var("p2").isa("person").has("phone-number", "+47 1234 1234 4"));
        curInsertB = var("rel").rel("caller", "p1").rel("callee", "p2").isa("call");
        singleThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(curMatchB, curInsertB));

        singleThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(null, null));

        pcgi.matchInsertThreadedInserting(singleThreadStatements, dataSession, 1, 1);

        // multi-thread inserting
        ProcessorStatement multiThreadStatements = new ProcessorStatement();

        multiThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(null, null));

        ArrayList<ThingVariable<?>> curMatchC = new ArrayList<>();
        ThingVariable<?> curInsertC;
        curMatchC.add(var("p1").isa("person").has("phone-number", "+47 1234 1234 1"));
        curMatchC.add(var("p2").isa("person").has("phone-number", "+47 1234 1234 3"));
        curInsertC = var("rel").rel("caller", "p1").rel("callee", "p2").isa("call");
        multiThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(curMatchC, curInsertC));

        multiThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(null, null));

        ArrayList<ThingVariable<?>> curMatchD = new ArrayList<>();
        ThingVariable<?> curInsertD;
        curMatchD.add(var("p1").isa("person").has("phone-number", "+47 1234 1234 2"));
        curMatchD.add(var("p2").isa("person").has("phone-number", "+47 1234 1234 4"));
        curInsertD = var("rel").rel("caller", "p1").rel("callee", "p2").isa("call");
        multiThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(curMatchD, curInsertD));

        multiThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(null, null));

        pcgi.matchInsertThreadedInserting(multiThreadStatements, dataSession, 4, 1);

        // pass nulls
        pcgi.matchInsertThreadedInserting(new ProcessorStatement(), dataSession, 1, 1);
        pcgi.matchInsertThreadedInserting(new ProcessorStatement(), dataSession, 4, 1);

        // tests
        GraknTransaction read = dataSession.transaction(GraknTransaction.Type.READ);

        GraqlMatch getQuery = Graql.match(var("c").isa("call")).get("c").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());

        getQuery = Graql.match(var("c").rel("caller", "p1").isa("call"),
                var("p1").has("phone-number", "+47 1234 1234 1")).get("c").limit(1000);
        Assert.assertEquals(2, read.query().match(getQuery).count());

        getQuery = Graql.match(var("c").rel("caller", "p1").isa("call"),
                var("p1").has("phone-number", "+47 1234 1234 3")).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("c").rel("caller", "p1").isa("call"),
                var("p1").has("phone-number", "+47 1234 1234 2")).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("c").rel("callee", "p1").isa("call"),
                var("p1").has("phone-number", "+47 1234 1234 2")).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("c").rel("callee", "p1").isa("call"),
                var("p1").has("phone-number", "+47 1234 1234 3")).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("c").rel("callee", "p1").isa("call"),
                var("p1").has("phone-number", "+47 1234 1234 4")).get("c").limit(1000);
        Assert.assertEquals(2, read.query().match(getQuery).count());

        read.close();
        dataSession.close();
        client.close();
    }

    @Test
    public void threadedAppendOrInsertGraknTest() throws InterruptedException {

        threadedInsertGraknTest();

        GraknClient client = pcgi.getClient();
        GraknSession dataSession = pcgi.getDataSession(client);

        // single-thread inserting
        ProcessorStatement singleThreadStatements = new ProcessorStatement();

        singleThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(null, null));
        singleThreadStatements.getInserts().add(null);

        ArrayList<ThingVariable<?>> curMatch = new ArrayList<>();
        ThingVariable<?> curInsert;
        ThingVariable<?> dirInsert;
        curMatch.add(var("p1").isa("person").has("phone-number", "+47 1234 1234 1"));
        curInsert = var("p1").has("nick-name", "appended-nick-name");
        dirInsert = var("p1").isa("person").has("phone-number", "+47 1234 1234 1").has("nick-name", "appended-nick-name");
        singleThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(curMatch, curInsert));
        singleThreadStatements.getInserts().add(dirInsert);

        singleThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(null, null));
        singleThreadStatements.getInserts().add(null);

        ArrayList<ThingVariable<?>> curMatchB = new ArrayList<>();
        ThingVariable<?> curInsertB;
        ThingVariable<?> dirInsertB;
        curMatchB.add(var("p1").isa("person").has("phone-number", "+47 1234 1234 5"));
        curInsertB = var("p1").has("nick-name", "newly-inserted-nick-name");
        dirInsertB = var("p1").isa("person").has("phone-number", "+47 1234 1234 5").has("nick-name", "newly-inserted-nick-name");
        singleThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(curMatchB, curInsertB));
        singleThreadStatements.getInserts().add(dirInsertB);

        singleThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(null, null));
        singleThreadStatements.getInserts().add(null);

        pcgi.appendOrInsertThreadedInserting(singleThreadStatements, dataSession, 1, 1);

        // multi-thread inserting
        ProcessorStatement multiThreadStatements = new ProcessorStatement();

        multiThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(null, null));
        multiThreadStatements.getInserts().add(null);

        ArrayList<ThingVariable<?>> curMatchC = new ArrayList<>();
        ThingVariable<?> curInsertC;
        ThingVariable<?> dirInsertC;
        curMatchC.add(var("p1").isa("person").has("phone-number", "+47 1234 1234 2"));
        curInsertC = var("p1").has("nick-name", "appended-nick-name-2");
        dirInsertC = var("p1").isa("person").has("phone-number", "+47 1234 1234 2").has("nick-name", "appended-nick-name-2");
        multiThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(curMatchC, curInsertC));
        multiThreadStatements.getInserts().add(dirInsertC);

        multiThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(null, null));
        multiThreadStatements.getInserts().add(null);

        ArrayList<ThingVariable<?>> curMatchD = new ArrayList<>();
        ThingVariable<?> curInsertD;
        ThingVariable<?> dirInsertD;
        curMatchD.add(var("p1").isa("person").has("phone-number", "+47 1234 1234 6"));
        curInsertD = var("p1").has("nick-name", "newly-inserted-nick-name-2");
        dirInsertD = var("p1").isa("person").has("phone-number", "+47 1234 1234 6").has("nick-name", "newly-inserted-nick-name-2");
        multiThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(curMatchD, curInsertD));
        multiThreadStatements.getInserts().add(dirInsertD);

        multiThreadStatements.getMatchInserts().add(new ProcessorStatement.MatchInsert(null, null));
        multiThreadStatements.getInserts().add(null);

        pcgi.appendOrInsertThreadedInserting(multiThreadStatements, dataSession, 4, 1);

        // pass nulls
        pcgi.appendOrInsertThreadedInserting(new ProcessorStatement(), dataSession, 1, 1);
        pcgi.appendOrInsertThreadedInserting(new ProcessorStatement(), dataSession, 4, 1);

        // tests
        GraknTransaction read = dataSession.transaction(GraknTransaction.Type.READ);

        GraqlMatch getQuery = Graql.match(var("p").isa("person")).get("p").limit(1000);
        Assert.assertEquals(6, read.query().match(getQuery).count());

        getQuery = Graql.match(var("p").isa("person").has("nick-name", var("nn"))).get("nn").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());

        getQuery = Graql.match(var("p").isa("person").has("phone-number", "+47 1234 1234 1").has("nick-name", var("v"))).get("v").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("p").isa("person").has("phone-number", "+47 1234 1234 2").has("nick-name", var("v"))).get("v").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("p").isa("person").has("phone-number", "+47 1234 1234 5").has("nick-name", var("v"))).get("v").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = Graql.match(var("p").isa("person").has("phone-number", "+47 1234 1234 6").has("nick-name", var("v"))).get("v").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        read.close();
        dataSession.close();
        client.close();
    }

}
