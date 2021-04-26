package loader;

import configuration.LoaderLoadConfig;
import grakn.client.api.GraknClient;
import grakn.client.api.GraknSession;
import grakn.client.api.GraknTransaction;
import graql.lang.Graql;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.pattern.variable.ThingVariable.Relation;
import graql.lang.pattern.variable.ThingVariable.Thing;
import graql.lang.pattern.variable.UnboundVariable;
import graql.lang.query.GraqlMatch;
import write.TypeDBWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static graql.lang.Graql.var;
import static test.TestUtil.getDT;
import static util.Util.getAbsPath;

public class MigrationTest {

    String graknURI = "localhost:1729";

    @Test
    public void migrateGenericTestsTest() throws IOException {

        String databaseName = "grami_generic_test";
        String asp = getAbsPath("src/test/resources/genericTests/schema-test.gql");
        String msp = getAbsPath("src/test/resources/genericTests/migrationStatus-test.json");
        String adcp = getAbsPath("src/test/resources/genericTests/dataConfig-test.json");
        String gcp = getAbsPath("src/test/resources/genericTests/processorConfig-test.json");

        LoaderLoadConfig loaderLoadConfig = new LoaderLoadConfig(graknURI, databaseName, asp, adcp, gcp);
        TypeDBLoader mig = new TypeDBLoader(loaderLoadConfig, msp, true);
        mig.migrate();
    }

    @Test
    public void migratePhoneCallsTest() throws IOException {

        String databaseName = "grami_phone_call_test";
        String asp = getAbsPath("src/test/resources/phoneCalls/schema.gql");
        String msp = getAbsPath("src/test/resources/phoneCalls/migrationStatus.json");
        String adcp = getAbsPath("src/test/resources/phoneCalls/dataConfig.json");
        String gcp = getAbsPath("src/test/resources/phoneCalls/processorConfig.json");

        LoaderLoadConfig loaderLoadConfig = new LoaderLoadConfig(graknURI, databaseName, asp, adcp, gcp);
        TypeDBLoader mig = new TypeDBLoader(loaderLoadConfig, msp, true);
        mig.migrate();

        TypeDBWriter gi = new TypeDBWriter(graknURI.split(":")[0], graknURI.split(":")[1], asp, databaseName);
        GraknClient client = gi.getClient();
        GraknSession session = gi.getDataSession(client);
        testEntities(session);
        testRelations(session);
        testNestedRelations(session);
        testAppendAttribute(session);
        testAttributes(session);
        testAttributeRelation(session);
        testInsertOrAppend(session);
        session.close();
        client.close();
    }

    public void testEntities(GraknSession session) {

        // query person by phone-number
        GraknTransaction read = session.transaction(GraknTransaction.Type.READ);
        GraqlMatch getQuery = Graql.match(var("p").isa("person").has("phone-number", "+261 860 539 4754")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        // query person by last name
        read = session.transaction(GraknTransaction.Type.READ);
        getQuery = Graql.match(var("p").isa("person").has("last-name", "Smith")).get("p").limit(1000);
        Assert.assertEquals(2, read.query().match(getQuery).count());

        // query all entities of type person
        read = session.transaction(GraknTransaction.Type.READ);
        getQuery = Graql.match(var("c").isa("person")).get("c").limit(1000);
        Assert.assertEquals(39, read.query().match(getQuery).count());

        // query all entites of type company
        read = session.transaction(GraknTransaction.Type.READ);
        getQuery = Graql.match(var("e").isa("company")).get("e").limit(1000);
        Assert.assertEquals(2, read.query().match(getQuery).count());

        read.close();
    }

    public void testRelations(GraknSession session) {

        // query call by duration
        GraknTransaction read = session.transaction(GraknTransaction.Type.READ);
        GraqlMatch getQuery = Graql.match(var("c").isa("call").has("duration", 2851)).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        // query call by date
        read = session.transaction(GraknTransaction.Type.READ);
        getQuery = Graql.match(var("c").isa("call").has("started-at", getDT("2018-09-17T18:43:42"))).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        // query call by caller
        read = session.transaction(GraknTransaction.Type.READ);
        Thing player = Graql.var("p").isa("person").has("phone-number", "+7 171 898 0853");
        Relation relation = Graql.var("c").isa("call").toUnbound().rel("caller", "p");
        ArrayList<ThingVariable> statements = new ArrayList<>();
        statements.add(player);
        statements.add(relation);
//        getQuery = Graql.match(statements);
        getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(14, read.query().match(getQuery).count());

        // query call by callee
        read = session.transaction(GraknTransaction.Type.READ);
        player = Graql.var("p").isa("person").has("phone-number", "+7 171 898 0853");
        relation = Graql.var("c").isa("call").toUnbound().rel("callee", "p");
        statements = new ArrayList<>();
        statements.add(player);
        statements.add(relation);
        getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());

        // query call by caller & callee
        read = session.transaction(GraknTransaction.Type.READ);
        Thing playerOne = Graql.var("p1").isa("person").has("phone-number", "+7 171 898 0853");
        Thing playerTwo = Graql.var("p2").isa("person").has("phone-number", "+57 629 420 5680");
        relation = Graql.var("c").isa("call").toUnbound().rel("caller", "p1").rel("callee", "p2");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());

        read.close();
    }

    public void testNestedRelations(GraknSession session) {

        // query specific communication-channel and count the number of past calls (single past-call):
        GraknTransaction read = session.transaction(GraknTransaction.Type.READ);
        Thing playerOne = Graql.var("p1").isa("person").has("phone-number", "+54 398 559 0423");
        Thing playerTwo = Graql.var("p2").isa("person").has("phone-number", "+48 195 624 2025");
        Relation relation = Graql.var("c").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x").isa("communication-channel");
        ArrayList<ThingVariable<?>> statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        GraqlMatch getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());
        getQuery = Graql.match(statements).get("x").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        // query specific communication-channel and count the number of past calls (listSeparated past-calls:
        read = session.transaction(GraknTransaction.Type.READ);
        playerOne = Graql.var("p1").isa("person").has("phone-number", "+263 498 495 0617");
        playerTwo = Graql.var("p2").isa("person").has("phone-number", "+33 614 339 0298");
        relation = Graql.var("c").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x").isa("communication-channel");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());
        getQuery = Graql.match(statements).get("x").limit(1000);
        Assert.assertEquals(6, read.query().match(getQuery).count());

        // make sure that this doesn't get inserted:
        read = session.transaction(GraknTransaction.Type.READ);
        playerOne = Graql.var("p1").isa("person").has("phone-number", "+7 690 597 4443");
        playerTwo = Graql.var("p2").isa("person").has("phone-number", "+54 398 559 9999");
        relation = Graql.var("c").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x").isa("communication-channel");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(0, read.query().match(getQuery).count());
        getQuery = Graql.match(statements).get("x").limit(1000);
        Assert.assertEquals(0, read.query().match(getQuery).count());

        // these are added by doing player matching for past calls:
        read = session.transaction(GraknTransaction.Type.READ);
        playerOne = Graql.var("p1").isa("person").has("phone-number", "+81 308 988 7153");
        playerTwo = Graql.var("p2").isa("person").has("phone-number", "+351 515 605 7915");
        relation = Graql.var("c").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x").isa("communication-channel");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(5, read.query().match(getQuery).count());
        getQuery = Graql.match(statements).get("x").limit(1000);
        Assert.assertEquals(5, read.query().match(getQuery).count());

        read = session.transaction(GraknTransaction.Type.READ);
        playerOne = Graql.var("p1").isa("person").has("phone-number", "+7 171 898 0853");
        playerTwo = Graql.var("p2").isa("person").has("phone-number", "+57 629 420 5680");
        relation = Graql.var("c").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x").isa("communication-channel");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());
        getQuery = Graql.match(statements).get("x").limit(1000);
        Assert.assertEquals(4, read.query().match(getQuery).count());

        // these must not be found (come from player-matched past-call):
        read = session.transaction(GraknTransaction.Type.READ);
        playerOne = Graql.var("p1").isa("person").has("phone-number", "+261 860 539 4754");
        relation = Graql.var("c").rel("peer", "p1").rel("past-call", "x").isa("communication-channel");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(relation);
        getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(0, read.query().match(getQuery).count());
        getQuery = Graql.match(statements).get("x").limit(1000);
        Assert.assertEquals(0, read.query().match(getQuery).count());

        read.close();
    }

    public void testAppendAttribute(GraknSession session) {

        // Count number of total inserts
        GraknTransaction read = session.transaction(GraknTransaction.Type.READ);
        GraqlMatch.Limited getQuery = Graql.match(var("p").isa("person").has("twitter-username", var("x"))).get("p").limit(1000);
        Assert.assertEquals(6, read.query().match(getQuery).count());

        // Count multi-write using listSeparator
        getQuery = Graql.match(var("p").isa("person").has("phone-number", "+263 498 495 0617").has("twitter-username", var("x"))).get("x").limit(1000);
        Assert.assertEquals(2, read.query().match(getQuery).count());

        //test relation total inserts
        getQuery = Graql.match(var("c").isa("call").has("call-rating", var("cr"))).get("c").limit(1000);
        Assert.assertEquals(5, read.query().match(getQuery).count());

        // specific relation write
        getQuery = Graql.match(var("c").isa("call").has("started-at", getDT("2018-09-24T03:16:48")).has("call-rating", var("cr"))).get("cr").limit(1000);
        read.query().match(getQuery).forEach(answer -> {
            Assert.assertEquals(5L, answer.get("cr").asAttribute().getValue());
        });

        read.close();
    }

    public void testAttributes(GraknSession session) {

        GraknTransaction read = session.transaction(GraknTransaction.Type.READ);
        GraqlMatch getQuery = Graql.match(var("a").isa("is-in-use")).get("a");
        Assert.assertEquals(3, read.query().match(getQuery).count());

        read = session.transaction(GraknTransaction.Type.READ);
        getQuery = Graql.match(var("a").eq("yes").isa("is-in-use")).get("a");
        Assert.assertEquals(1, read.query().match(getQuery).count());

        read = session.transaction(GraknTransaction.Type.READ);
        getQuery = Graql.match(var("a").eq("no").isa("is-in-use")).get("a");
        Assert.assertEquals(1, read.query().match(getQuery).count());
        read.close();
    }

    public void testAttributeRelation(GraknSession session) {

        GraknTransaction read = session.transaction(GraknTransaction.Type.READ);
        GraqlMatch getQuery = Graql.match(var("a").isa("in-use")).get("a");
        Assert.assertEquals(7, read.query().match(getQuery).count());

        read.close();
    }

    public void testInsertOrAppend(GraknSession session) {
        GraknTransaction read = session.transaction(GraknTransaction.Type.READ);
        GraqlMatch getQuery = Graql.match(var("e").isa("person").has("nick-name", UnboundVariable.named("x"))).get("e");
        Assert.assertEquals(8, read.query().match(getQuery).count());

        // test new ones present (middle and at end)
        read = session.transaction(GraknTransaction.Type.READ);
        getQuery = Graql.match(var("p").isa("person").has("first-name", "Naruto")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        read = session.transaction(GraknTransaction.Type.READ);
        getQuery = Graql.match(var("p").isa("person").has("first-name", "Sasuke")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        read = session.transaction(GraknTransaction.Type.READ);
        getQuery = Graql.match(var("p").isa("person").has("first-name", "Sakura")).get("p").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery).count());

        read.close();
    }

    @Test
    public void issue10Test() throws IOException {

        String databaseName = "issue10";
        String asp = getAbsPath("src/test/resources/bugfixing/issue10/schema.gql");
        String msp = getAbsPath("src/test/resources/bugfixing/issue10/migrationStatus.json");
        String adcp = getAbsPath("src/test/resources/bugfixing/issue10/dataConfig.json");
        String gcp = getAbsPath("src/test/resources/bugfixing/issue10/processorConfig.json");

        LoaderLoadConfig loaderLoadConfig = new LoaderLoadConfig(graknURI, databaseName, asp, adcp, gcp);
        TypeDBLoader mig = new TypeDBLoader(loaderLoadConfig, msp, true);
        mig.migrate();
    }
}

