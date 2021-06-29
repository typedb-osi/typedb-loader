package write;

import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.query.TypeQLDelete;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import processor.InsertQueries;
import org.junit.Assert;
import org.junit.Test;
import util.Util;

import java.util.ArrayList;

public class TypeDBWriterTest {

    TypeDBWriter gi;
    TypeDBWriter pcgi;
    String databaseName = "grakn_inserter_test";
    String schemaPath;

    public TypeDBWriterTest() {
        this.schemaPath = Util.getAbsPath("src/test/resources/genericTests/schema-test.gql");
        this.gi = new TypeDBWriter("localhost", "1729", schemaPath, databaseName);
        this.pcgi = new TypeDBWriter("localhost", "1729", "src/test/resources/phoneCalls/schema-updated.gql", databaseName);
    }

    @Test
    public void reloadKeyspaceTest() {
        TypeDBClient client = gi.getClient();

        gi.cleanAndDefineSchemaToDatabase(client);
        Assert.assertTrue(client.databases().contains(databaseName));

        //ensure Keyspace contains schema
        TypeDBSession dataSession = gi.getDataSession(client);
        TypeDBTransaction read = dataSession.transaction(TypeDBTransaction.Type.READ);
        TypeQLMatch.Limited mq = TypeQL.match(TypeQL.var("e").sub("entity")).get("e").limit(100);
        Assert.assertEquals(4, read.query().match(mq).count());
        read.close();

        read = dataSession.transaction(TypeDBTransaction.Type.READ);
        mq = TypeQL.match(TypeQL.var("e").type("entity1")).get("e").limit(100);
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

        TypeDBClient client = gi.getClient();
        gi.cleanAndDefineSchemaToDatabase(client);
        TypeDBSession dataSession = gi.getDataSession(client);

        //perform data entry
        TypeDBTransaction write = dataSession.transaction(TypeDBTransaction.Type.WRITE);
        TypeQLInsert insertQuery = TypeQL.insert(TypeQL.var("e").isa("entity1").has("entity1-id", "ide1"));
        write.query().insert(insertQuery);
        write.commit();
        write.close();

        //ensure graph contains our write
        TypeDBTransaction read = dataSession.transaction(TypeDBTransaction.Type.READ);
        TypeQLMatch.Limited getQuery = TypeQL.match(TypeQL.var("e").isa("entity1").has("entity1-id", "ide1")).get("e").limit(100);
        Assert.assertEquals(1, read.query().match(getQuery).count());
        read.close();

        read = dataSession.transaction(TypeDBTransaction.Type.READ);
        getQuery = TypeQL.match(TypeQL.var("e").isa("entity1").has("entity1-id", "ide2")).limit(100);
        Assert.assertEquals(0, read.query().match(getQuery).count());
        read.close();

        //another test for our write
        read = dataSession.transaction(TypeDBTransaction.Type.READ);
        getQuery = TypeQL.match(TypeQL.var("e").isa("entity1").has("entity1-id", "ide1")).get("e").limit(100);
        read.query().match(getQuery).forEach(answers -> answers.concepts().forEach(entry -> Assert.assertTrue(entry.isEntity())));
        read.close();

        //clean up:
        TypeDBTransaction delete = dataSession.transaction(TypeDBTransaction.Type.WRITE);
        TypeQLDelete delQuery = TypeQL.match(
                TypeQL.var("e").isa("entity1").has("entity1-id", "ide1")
        ).delete(TypeQL.var("e").isa("entity1"));
        delete.query().delete(delQuery);
        delete.commit();
        delete.close();

        dataSession.close();
        client.close();
    }

    @Test
    public void threadedInsertGraknTest() throws InterruptedException {

        TypeDBClient client = pcgi.getClient();
        pcgi.cleanAndDefineSchemaToDatabase(client);
        TypeDBSession dataSession = pcgi.getDataSession(client);

        ArrayList<ThingVariable<?>> singleThreadStatements = new ArrayList<>();
        singleThreadStatements.add(null);
        singleThreadStatements.add(TypeQL.var("person").isa("person").has("first-name", "first-first-name").has("phone-number", "+47 1234 1234 1"));
        singleThreadStatements.add(null);
        singleThreadStatements.add(TypeQL.var("person").isa("person").has("first-name", "second-first-name").has("phone-number", "+47 1234 1234 2"));
        singleThreadStatements.add(null);
        pcgi.insertThreadedInserting(singleThreadStatements, dataSession, 1, 1);

        ArrayList<ThingVariable<?>> multiThreadStatements = new ArrayList<>();
        multiThreadStatements.add(null);
        multiThreadStatements.add(TypeQL.var("person").isa("person").has("first-name", "first-first-name-mt").has("phone-number", "+47 1234 1234 3"));
        multiThreadStatements.add(null);
        multiThreadStatements.add(TypeQL.var("person").isa("person").has("first-name", "second-first-name-mt").has("phone-number", "+47 1234 1234 4"));
        multiThreadStatements.add(null);
        pcgi.insertThreadedInserting(multiThreadStatements, dataSession, 4, 1);

        pcgi.insertThreadedInserting(new ArrayList<>(), dataSession, 1, 1);
        pcgi.insertThreadedInserting(new ArrayList<>(), dataSession, 4, 1);

        TypeDBTransaction read = dataSession.transaction(TypeDBTransaction.Type.READ);

        TypeQLMatch getQuery = TypeQL.match(TypeQL.var("p").isa("person")).get("p").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("first-name", "first-first-name")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("first-name", "second-first-name")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("first-name", "first-first-name-mt")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("first-name", "first-first-name-mt")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("phone-number", "+47 1234 1234 1")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("phone-number", "+47 1234 1234 2")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("phone-number", "+47 1234 1234 3")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("phone-number", "+47 1234 1234 4")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        read.close();
        dataSession.close();
        client.close();
    }

    @Test
    public void threadedMatchInsertGraknTest() throws InterruptedException {

        threadedInsertGraknTest();

        TypeDBClient client = pcgi.getClient();
        TypeDBSession dataSession = pcgi.getDataSession(client);

        // single-thread inserting
        InsertQueries singleThreadStatements = new InsertQueries();

        singleThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(null, null));

        ArrayList<ThingVariable<?>> curMatch = new ArrayList<>();
        ThingVariable<?> curInsert;
        curMatch.add(TypeQL.var("p1").isa("person").has("phone-number", "+47 1234 1234 1"));
        curMatch.add(TypeQL.var("p2").isa("person").has("phone-number", "+47 1234 1234 2"));
        curInsert = TypeQL.var("rel").rel("caller", "p1").rel("callee", "p2").isa("call");
        singleThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(curMatch, curInsert));

        singleThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(null, null));

        ArrayList<ThingVariable<?>> curMatchB = new ArrayList<>();
        ThingVariable<?> curInsertB;
        curMatchB.add(TypeQL.var("p1").isa("person").has("phone-number", "+47 1234 1234 3"));
        curMatchB.add(TypeQL.var("p2").isa("person").has("phone-number", "+47 1234 1234 4"));
        curInsertB = TypeQL.var("rel").rel("caller", "p1").rel("callee", "p2").isa("call");
        singleThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(curMatchB, curInsertB));

        singleThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(null, null));

        pcgi.matchInsertThreadedInserting(singleThreadStatements, dataSession, 1, 1);

        // multi-thread inserting
        InsertQueries multiThreadStatements = new InsertQueries();

        multiThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(null, null));

        ArrayList<ThingVariable<?>> curMatchC = new ArrayList<>();
        ThingVariable<?> curInsertC;
        curMatchC.add(TypeQL.var("p1").isa("person").has("phone-number", "+47 1234 1234 1"));
        curMatchC.add(TypeQL.var("p2").isa("person").has("phone-number", "+47 1234 1234 3"));
        curInsertC = TypeQL.var("rel").rel("caller", "p1").rel("callee", "p2").isa("call");
        multiThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(curMatchC, curInsertC));

        multiThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(null, null));

        ArrayList<ThingVariable<?>> curMatchD = new ArrayList<>();
        ThingVariable<?> curInsertD;
        curMatchD.add(TypeQL.var("p1").isa("person").has("phone-number", "+47 1234 1234 2"));
        curMatchD.add(TypeQL.var("p2").isa("person").has("phone-number", "+47 1234 1234 4"));
        curInsertD = TypeQL.var("rel").rel("caller", "p1").rel("callee", "p2").isa("call");
        multiThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(curMatchD, curInsertD));

        multiThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(null, null));

        pcgi.matchInsertThreadedInserting(multiThreadStatements, dataSession, 4, 1);

        // pass nulls
        pcgi.matchInsertThreadedInserting(new InsertQueries(), dataSession, 1, 1);
        pcgi.matchInsertThreadedInserting(new InsertQueries(), dataSession, 4, 1);

        // tests
        TypeDBTransaction read = dataSession.transaction(TypeDBTransaction.Type.READ);

        TypeQLMatch getQuery = TypeQL.match(TypeQL.var("c").isa("call")).get("c").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("c").rel("caller", "p1").isa("call"),
                TypeQL.var("p1").has("phone-number", "+47 1234 1234 1")).get("c").limit(1000);
        Assert.assertEquals(2, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("c").rel("caller", "p1").isa("call"),
                TypeQL.var("p1").has("phone-number", "+47 1234 1234 3")).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("c").rel("caller", "p1").isa("call"),
                TypeQL.var("p1").has("phone-number", "+47 1234 1234 2")).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("c").rel("callee", "p1").isa("call"),
                TypeQL.var("p1").has("phone-number", "+47 1234 1234 2")).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("c").rel("callee", "p1").isa("call"),
                TypeQL.var("p1").has("phone-number", "+47 1234 1234 3")).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("c").rel("callee", "p1").isa("call"),
                TypeQL.var("p1").has("phone-number", "+47 1234 1234 4")).get("c").limit(1000);
        Assert.assertEquals(2, read.query().match(getQuery).count());

        read.close();
        dataSession.close();
        client.close();
    }

    @Test
    public void threadedAppendOrInsertGraknTest() throws InterruptedException {

        threadedInsertGraknTest();

        //TODO: reproduce RocksDB Busy Exception when two separate threads try to append to same concept

        TypeDBClient client = pcgi.getClient();
        TypeDBSession dataSession = pcgi.getDataSession(client);

        // single-thread inserting
        InsertQueries singleThreadStatements = new InsertQueries();

        singleThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(null, null));
        singleThreadStatements.getDirectInserts().add(null);

        ArrayList<ThingVariable<?>> curMatch = new ArrayList<>();
        ThingVariable<?> curInsert;
        ThingVariable<?> dirInsert;
        curMatch.add(TypeQL.var("p1").isa("person").has("phone-number", "+47 1234 1234 1"));
        curInsert = TypeQL.var("p1").has("nick-name", "appended-nick-name");
        dirInsert = TypeQL.var("p1").isa("person").has("phone-number", "+47 1234 1234 1").has("nick-name", "appended-nick-name");
        singleThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(curMatch, curInsert));
        singleThreadStatements.getDirectInserts().add(dirInsert);

        singleThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(null, null));
        singleThreadStatements.getDirectInserts().add(null);

        ArrayList<ThingVariable<?>> curMatchB = new ArrayList<>();
        ThingVariable<?> curInsertB;
        ThingVariable<?> dirInsertB;
        curMatchB.add(TypeQL.var("p1").isa("person").has("phone-number", "+47 1234 1234 5"));
        curInsertB = TypeQL.var("p1").has("nick-name", "newly-inserted-nick-name");
        dirInsertB = TypeQL.var("p1").isa("person").has("phone-number", "+47 1234 1234 5").has("nick-name", "newly-inserted-nick-name");
        singleThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(curMatchB, curInsertB));
        singleThreadStatements.getDirectInserts().add(dirInsertB);

        singleThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(null, null));
        singleThreadStatements.getDirectInserts().add(null);

        pcgi.appendOrInsertThreadedInserting(singleThreadStatements, dataSession, 1, 1);

        // multi-thread inserting
        InsertQueries multiThreadStatements = new InsertQueries();

        multiThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(null, null));
        multiThreadStatements.getDirectInserts().add(null);

        ArrayList<ThingVariable<?>> curMatchC = new ArrayList<>();
        ThingVariable<?> curInsertC;
        ThingVariable<?> dirInsertC;
        curMatchC.add(TypeQL.var("p1").isa("person").has("phone-number", "+47 1234 1234 2"));
        curInsertC = TypeQL.var("p1").has("nick-name", "appended-nick-name-2");
        dirInsertC = TypeQL.var("p1").isa("person").has("phone-number", "+47 1234 1234 2").has("nick-name", "appended-nick-name-2");
        multiThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(curMatchC, curInsertC));
        multiThreadStatements.getDirectInserts().add(dirInsertC);

        multiThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(null, null));
        multiThreadStatements.getDirectInserts().add(null);

        ArrayList<ThingVariable<?>> curMatchD = new ArrayList<>();
        ThingVariable<?> curInsertD;
        ThingVariable<?> dirInsertD;
        curMatchD.add(TypeQL.var("p1").isa("person").has("phone-number", "+47 1234 1234 6"));
        curInsertD = TypeQL.var("p1").has("nick-name", "newly-inserted-nick-name-2");
        dirInsertD = TypeQL.var("p1").isa("person").has("phone-number", "+47 1234 1234 6").has("nick-name", "newly-inserted-nick-name-2");
        multiThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(curMatchD, curInsertD));
        multiThreadStatements.getDirectInserts().add(dirInsertD);

        multiThreadStatements.getMatchInserts().add(new InsertQueries.MatchInsert(null, null));
        multiThreadStatements.getDirectInserts().add(null);

        pcgi.appendOrInsertThreadedInserting(multiThreadStatements, dataSession, 4, 1);

        // pass nulls
        pcgi.appendOrInsertThreadedInserting(new InsertQueries(), dataSession, 1, 1);
        pcgi.appendOrInsertThreadedInserting(new InsertQueries(), dataSession, 4, 1);

        // tests
        TypeDBTransaction read = dataSession.transaction(TypeDBTransaction.Type.READ);

        TypeQLMatch getQuery = TypeQL.match(TypeQL.var("p").isa("person")).get("p").limit(1000);
        Assert.assertEquals(6, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("nick-name", TypeQL.var("nn"))).get("nn").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("phone-number", "+47 1234 1234 1").has("nick-name", TypeQL.var("v"))).get("v").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("phone-number", "+47 1234 1234 2").has("nick-name", TypeQL.var("v"))).get("v").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("phone-number", "+47 1234 1234 5").has("nick-name", TypeQL.var("v"))).get("v").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("phone-number", "+47 1234 1234 6").has("nick-name", TypeQL.var("v"))).get("v").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        read.close();
        dataSession.close();
        client.close();
    }

}
