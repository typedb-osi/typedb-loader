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
        testNestedRelations(session);

        session.close();
        client.close();
    }

    public void testAttributes(TypeDBSession session) {
        TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ);
        TypeQLMatch getQuery = TypeQL.match(TypeQL.var("a").isa("is-in-use")).get("a");
        Assert.assertEquals(3, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("a").eq("yes").isa("is-in-use")).get("a");
        Assert.assertEquals(1, read.query().match(getQuery).count());

        getQuery = TypeQL.match(TypeQL.var("a").eq("no").isa("is-in-use")).get("a");
        Assert.assertEquals(1, read.query().match(getQuery).count());


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
        getQuery = TypeQL.match(TypeQL.var("p").isa("person").has("last-name", "Smith")).get("p").limit(1000);
        Assert.assertEquals(2, read.query().match(getQuery).count());

        // query all entities of type person
        getQuery = TypeQL.match(TypeQL.var("c").isa("person")).get("c").limit(1000);
        Assert.assertEquals(33, read.query().match(getQuery).count());

        // query all entites of type company
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
        getQuery = TypeQL.match(TypeQL.var("c").isa("call").has("started-at", getDT("2018-09-17T18:43:42"))).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        // query call by caller
        ThingVariable.Thing player = TypeQL.var("p").isa("person").has("phone-number", "+7 171 898 0853");
        ThingVariable.Relation relation = TypeQL.var("c").isa("call").toUnbound().rel("caller", "p");
        ArrayList<ThingVariable<?>> statements = new ArrayList<>();
        statements.add(player);
        statements.add(relation);
        getQuery = TypeQL.match(statements).get("c").limit(1000);
        Assert.assertEquals(14, read.query().match(getQuery).count());

        // query call by callee
        player = TypeQL.var("p").isa("person").has("phone-number", "+7 171 898 0853");
        relation = TypeQL.var("c").isa("call").toUnbound().rel("callee", "p");
        statements = new ArrayList<>();
        statements.add(player);
        statements.add(relation);
        getQuery = TypeQL.match(statements).get("c").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());

        // query call by caller & callee
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

    public void testNestedRelations(TypeDBSession session) {

        // query specific communication-channel and count the number of past calls (single past-call):
        TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ);
        ThingVariable.Thing playerOne = TypeQL.var("p1").isa("person").has("phone-number", "+54 398 559 0423");
        ThingVariable.Thing playerTwo = TypeQL.var("p2").isa("person").has("phone-number", "+48 195 624 2025");
        ThingVariable.Relation relation = TypeQL.var("c").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x").isa("communication-channel");
        ArrayList<ThingVariable<?>> statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        TypeQLMatch getQuery = TypeQL.match(statements).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());
        getQuery = TypeQL.match(statements).get("x").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        // query specific communication-channel and count the number of past calls (listSeparated past-calls:
        playerOne = TypeQL.var("p1").isa("person").has("phone-number", "+263 498 495 0617");
        playerTwo = TypeQL.var("p2").isa("person").has("phone-number", "+33 614 339 0298");
        relation = TypeQL.var("c").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x").isa("communication-channel");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = TypeQL.match(statements).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());
        getQuery = TypeQL.match(statements).get("x").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        // make sure that this doesn't get inserted:
        playerOne = TypeQL.var("p1").isa("person").has("phone-number", "+7 690 597 4443");
        playerTwo = TypeQL.var("p2").isa("person").has("phone-number", "+54 398 559 9999");
        relation = TypeQL.var("c").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x").isa("communication-channel");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = TypeQL.match(statements).get("c").limit(1000);
        Assert.assertEquals(0, read.query().match(getQuery).count());
        getQuery = TypeQL.match(statements).get("x").limit(1000);
        Assert.assertEquals(0, read.query().match(getQuery).count());

        // these are added by doing player matching for past calls:
        playerOne = TypeQL.var("p1").isa("person").has("phone-number", "+81 308 988 7153");
        playerTwo = TypeQL.var("p2").isa("person").has("phone-number", "+351 515 605 7915");
        relation = TypeQL.var("c").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x").isa("communication-channel");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = TypeQL.match(statements).get("c").limit(1000);
        Assert.assertEquals(5, read.query().match(getQuery).count());
        getQuery = TypeQL.match(statements).get("x").limit(1000);
        Assert.assertEquals(5, read.query().match(getQuery).count());

        read = session.transaction(TypeDBTransaction.Type.READ);
        playerOne = TypeQL.var("p1").isa("person").has("phone-number", "+7 171 898 0853");
        playerTwo = TypeQL.var("p2").isa("person").has("phone-number", "+57 629 420 5680");
        relation = TypeQL.var("c").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x").isa("communication-channel");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = TypeQL.match(statements).get("c").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());
        getQuery = TypeQL.match(statements).get("x").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());

        // these must not be found (come from player-matched past-call):
        read = session.transaction(TypeDBTransaction.Type.READ);
        playerOne = TypeQL.var("p1").isa("person").has("phone-number", "+261 860 539 4754");
        relation = TypeQL.var("c").rel("peer", "p1").rel("past-call", "x").isa("communication-channel");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(relation);
        getQuery = TypeQL.match(statements).get("c").limit(1000);
        Assert.assertEquals(0, read.query().match(getQuery).count());
        getQuery = TypeQL.match(statements).get("x").limit(1000);
        Assert.assertEquals(0, read.query().match(getQuery).count());

        read.close();
    }
}
