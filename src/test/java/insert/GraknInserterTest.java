package insert;

import grakn.client.GraknClient;
import grakn.client.GraknClient.Session;
import grakn.client.GraknClient.Transaction;
import graql.lang.Graql;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import org.junit.Assert;
import org.junit.Test;
import util.Util;

import static graql.lang.Graql.var;

public class GraknInserterTest {

    GraknInserter gi;
    String databaseName = "grakn_migrator_test";
    String schemaPath;

    public GraknInserterTest() {
        this.schemaPath = Util.getAbsPath("src/test/resources/genericTests/schema-test.gql");
        this.gi = new GraknInserter("localhost", "1729", schemaPath, databaseName);
    }

    @Test
    public void reloadKeyspaceTest() {
        GraknClient client = gi.getClient();

        gi.cleanAndDefineSchemaToDatabase(client);
        Assert.assertTrue(client.databases().contains(databaseName));

        //ensure Keyspace contains schema
        Session dataSession = gi.getDataSession(client);
        Transaction read = dataSession.transaction(Transaction.Type.READ);
        GraqlMatch.Limited mq = Graql.match(var("e").sub("entity")).get("e").limit(100);
        Assert.assertEquals(4, read.query().match(mq).count());
        read.close();

         read = dataSession.transaction(Transaction.Type.READ);
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

        //perform data entry
        Session dataSession = gi.getDataSession(client);
        Transaction write = dataSession.transaction(Transaction.Type.WRITE);
        GraqlInsert insertQuery = Graql.insert(var("e").isa("entity1").has("entity1-id", "ide1"));
        write.query().insert(insertQuery);
        write.commit();
        write.close();

        //ensure graph contains our insert
        Transaction read = dataSession.transaction(Transaction.Type.READ);
        GraqlMatch.Limited getQuery = Graql.match(var("e").isa("entity1").has("entity1-id", "ide1")).get("e").limit(100);
        Assert.assertEquals(1, read.query().match(getQuery).count());
        read.close();

        read = dataSession.transaction(Transaction.Type.READ);
        getQuery = Graql.match(var("e").isa("entity1").has("entity1-id", "ide2")).limit(100);
        Assert.assertEquals(0, read.query().match(getQuery).count());
        read.close();

        //another test for our insert
        read = dataSession.transaction(Transaction.Type.READ);
        getQuery = Graql.match(var("e").isa("entity1").has("entity1-id", "ide1")).get("e").limit(100);
        read.query().match(getQuery).forEach( answers -> {
            answers.concepts().stream().forEach( entry -> {
                Assert.assertTrue(entry.isEntity());
            });
        });
        read.close();

        //clean up:
        Transaction delete = dataSession.transaction(Transaction.Type.WRITE);
        GraqlDelete delQuery = Graql.match(
                var("e").isa("entity1").has("entity1-id", "ide1")
        ).delete(var("e").isa("entity1"));
        delete.query().delete(delQuery);
        delete.commit();
        delete.close();

        dataSession.close();
        client.close();
    }

}
