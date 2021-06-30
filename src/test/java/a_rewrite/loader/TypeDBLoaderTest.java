package a_rewrite.loader;

import a_rewrite.util.TypeDBUtil;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

import static test.TestUtil.getDT;

public class TypeDBLoaderTest {

    String graknUri = "localhost:1729";

    @Test
    public void loadSyntheticTest() {
        String dcPath = new File("src/test/resources/1.0.0/synthetic/dc.json").getAbsolutePath();
        String databaseName = "synthetic-test";
        TypeDBLoader typeDBLoader = new TypeDBLoader(dcPath, databaseName, graknUri);
        typeDBLoader.load();
    }

    @Test
    public void loadPhoneCallsTest() {
        String dcPath = new File("src/test/resources/1.0.0/phoneCalls/dc.json").getAbsolutePath();
        String databaseName = "phone-calls-test";
        TypeDBLoader typeDBLoader = new TypeDBLoader(dcPath, databaseName, graknUri);
        typeDBLoader.load();

        TypeDBClient client = TypeDBUtil.getClient(graknUri, 4);
        TypeDBSession session = TypeDBUtil.getDataSession(client, databaseName);

        testAttributes(session);
        testEntities(session);
        testRelations(session);
        testAttributeRelation(session);

        session.close();
        client.close();
    }

    public void testAttributes(TypeDBSession session) {
        TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ);
        TypeQLMatch getQuery = TypeQL.match(TypeQL.var("a").isa("is-in-use")).get("a");
        Assert.assertEquals(3, read.query().match(getQuery).count());

        read = session.transaction(TypeDBTransaction.Type.READ);
        getQuery = TypeQL.match(TypeQL.var("a").eq("yes").isa("is-in-use")).get("a");
        Assert.assertEquals(1, read.query().match(getQuery).count());

        read = session.transaction(TypeDBTransaction.Type.READ);
        getQuery = TypeQL.match(TypeQL.var("a").eq("no").isa("is-in-use")).get("a");
        Assert.assertEquals(1, read.query().match(getQuery).count());
        read.close();

        read = session.transaction(TypeDBTransaction.Type.READ);
        getQuery = TypeQL.match(TypeQL.var("a").eq("5").isa("is-in-use")).get("a");
        Assert.assertEquals(1, read.query().match(getQuery).count());
        read.close();
    }

    public void testEntities(TypeDBSession session) {

        // query person by phone-number
        TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ);
        TypeQLMatch getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("phone-number", "+261 860 539 4754")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        // query person by last name
        read = session.transaction(TypeDBTransaction.Type.READ);
        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("last-name", "Smith")).get("p").limit(1000);
        Assert.assertEquals(2, read.query().match(getQuery).count());

        // query all entities of type person
        read = session.transaction(TypeDBTransaction.Type.READ);
        getQuery = TypeQL.match(TypeQL.var("c").isa("person")).get("c").limit(1000);
        Assert.assertEquals(33, read.query().match(getQuery).count());

        // query all entites of type company
        read = session.transaction(TypeDBTransaction.Type.READ);
        getQuery = TypeQL.match(TypeQL.var("e").isa("company")).get("e").limit(1000);
        Assert.assertEquals(2, read.query().match(getQuery).count());

        read.close();
    }

    public void testRelations(TypeDBSession session) {

        // query call by duration
        TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ);
        TypeQLMatch getQuery = TypeQL.match(TypeQL.var("c").isa("call").has("duration", 2851)).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        // query call by date
        read = session.transaction(TypeDBTransaction.Type.READ);
        getQuery = TypeQL.match(TypeQL.var("c").isa("call").has("started-at", getDT("2018-09-17T18:43:42"))).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        // query call by caller
        read = session.transaction(TypeDBTransaction.Type.READ);
        ThingVariable.Thing player = TypeQL.var("p").isa("person").has("phone-number", "+7 171 898 0853");
        ThingVariable.Relation relation = TypeQL.var("c").isa("call").toUnbound().rel("caller", "p");
        ArrayList<ThingVariable<?>> statements = new ArrayList<>();
        statements.add(player);
        statements.add(relation);
        getQuery = TypeQL.match(statements).get("c").limit(1000);
        Assert.assertEquals(14, read.query().match(getQuery).count());

        // query call by callee
        read = session.transaction(TypeDBTransaction.Type.READ);
        player = TypeQL.var("p").isa("person").has("phone-number", "+7 171 898 0853");
        relation = TypeQL.var("c").isa("call").toUnbound().rel("callee", "p");
        statements = new ArrayList<>();
        statements.add(player);
        statements.add(relation);
        getQuery = TypeQL.match(statements).get("c").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());

        // query call by caller & callee
        read = session.transaction(TypeDBTransaction.Type.READ);
        ThingVariable.Thing playerOne = TypeQL.var("p1").isa("person").has("phone-number", "+7 171 898 0853");
        ThingVariable.Thing playerTwo = TypeQL.var("p2").isa("person").has("phone-number", "+57 629 420 5680");
        relation = TypeQL.var("c").isa("call").toUnbound().rel("caller", "p1").rel("callee", "p2");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = TypeQL.match(statements).get("c").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());

        read.close();
    }

    public void testAttributeRelation(TypeDBSession session) {

        TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ);
        TypeQLMatch getQuery = TypeQL.match(TypeQL.var("a").isa("in-use")).get("a");
        Assert.assertEquals(7, read.query().match(getQuery).count());

        read.close();
    }
}
