package insert;

import grakn.client.GraknClient;
import grakn.client.answer.ConceptMap;
import graql.lang.Graql;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlInsert;
import org.junit.Assert;
import org.junit.Test;
import util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static graql.lang.Graql.parse;
import static graql.lang.Graql.var;

public class GraknInserterTest {

    GraknInserter gm;
    String keyspaceName = "grakn_migrator_test";

    public GraknInserterTest() {
        String schemaPath = Util.getAbsPath("src/test/resources/genericTests/schema-test.gql");
        this.gm = new GraknInserter("localhost", "48555", schemaPath, keyspaceName);
    }

    @Test
    public void reloadKeyspaceTest() {
        GraknClient client = gm.getClient();
        GraknClient.Session session = client.session(keyspaceName);

        session = gm.setKeyspaceToSchema(client, session);
        Assert.assertTrue(client.keyspaces().retrieve().contains(keyspaceName));

        //ensure Keyspace contains schema
        GraknClient.Transaction read = session.transaction().read();
        GraqlGet getQuery = Graql.match(var("e").sub("entity")).get().limit(3);
        Assert.assertEquals(3, read.stream(getQuery).get().count());
        read.close();

        session.close();
        client.close();
    }

    @Test
    public void loadSchemaFromFileTest() {
        String schema = gm.loadSchemaFromFile();

        Assert.assertTrue("schema test positive", schema.contains("entity1 sub entity"));
        Assert.assertFalse("schema test negative", schema.contains("entity99 sub entity"));
    }

    @Test
    public void insertToGraknTest() {

        GraknClient client = gm.getClient();
        GraknClient.Session session = client.session(keyspaceName);

        session = gm.setKeyspaceToSchema(client, session);

        //perform data entry
        GraknClient.Transaction write = session.transaction().write();
        GraqlInsert insertQuery = Graql.insert(var("e").isa("entity1").has("entity1-id", "ide1"));
        write.execute(insertQuery);
        write.commit();

        //ensure graph contains our insert
        GraknClient.Transaction read = session.transaction().read();
        GraqlGet getQuery = Graql.match(var("e").isa("entity1").has("entity1-id", "ide1")).get().limit(1);
        Assert.assertEquals(1, read.stream(getQuery).get().count());
        read.close();

        read = session.transaction().read();
        getQuery = Graql.match(var("e").isa("entity1").has("entity1-id", "ide2")).get().limit(1);
        Assert.assertEquals(0, read.stream(getQuery).get().count());
        read.close();

        //another test for our insert
        List<String> queryList = Arrays.asList(
                "match ",
                " $e isa entity1, has entity1-id \"ide1\";",
                " get $e;"
        );
        read = session.transaction().read();
        List<String> res = new ArrayList<String>();
        List<ConceptMap> answers = read.execute((GraqlGet) parse(String.join("", queryList))).get();
        GraknClient.Session finalSession = session;
        answers.forEach(a -> {
            finalSession.transaction().read().getConcept(a.get("e").id()).asThing().attributes().forEach(l -> {
                System.out.println(l.value());
                res.add(l.value().toString());
            });
        });
        Assert.assertEquals("ide1", res.get(0));
        read.close();

        //clean up:
        GraknClient.Transaction delete = session.transaction().write();
        GraqlDelete delQuery = Graql.match(
                var("e").isa("entity1").has("entity1-id", "ide1")
        ).delete(var("e").isa("entity1"));
        delete.execute(delQuery);
        delQuery = Graql.match(
                var("a").isa("entity1-id").val("ide1")
        ).delete(var("a").isa("entity1-id"));
        delete.execute(delQuery);
        delete.commit();

        session.close();
        client.close();
    }

}
