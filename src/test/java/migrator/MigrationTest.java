package migrator;

import configuration.MigrationConfig;
import grakn.client.GraknClient;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import graql.lang.statement.StatementInstance;
import insert.GraknInserter;
import org.junit.Assert;
import org.junit.Test;

import static graql.lang.Graql.var;
import static util.Util.getAbsPath;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

public class MigrationTest {

    @Test
    public void migrateGenericTestsTest() throws IOException {

        String keyspaceName = "grami_generic_test";
        String asp = getAbsPath("src/test/resources/genericTests/schema-test.gql");
        String msp = getAbsPath("src/test/resources/genericTests/migrationStatus-test.json");
        String adcp = getAbsPath("src/test/resources/genericTests/dataConfig-test.json");
        String gcp = getAbsPath("src/test/resources/genericTests/processorConfig-test.json");

        MigrationConfig migrationConfig = new MigrationConfig("localhost:48555",keyspaceName, asp, adcp, gcp);
        GraknMigrator mig = new GraknMigrator(migrationConfig, msp, true);
        mig.migrate(true, true, true);
    }

    @Test
    public void migratePhoneCallsTest() throws IOException {

        String keyspaceName = "grami_phone_call_test";
        String asp = getAbsPath("src/test/resources/phone-calls/schema.gql");
        String msp = getAbsPath("src/test/resources/phone-calls/migrationStatus.json");
        String adcp = getAbsPath("src/test/resources/phone-calls/dataConfig.json");
        String gcp = getAbsPath("src/test/resources/phone-calls/processorConfig.json");

        MigrationConfig migrationConfig = new MigrationConfig("localhost:48555",keyspaceName, asp, adcp, gcp);
        GraknMigrator mig = new GraknMigrator(migrationConfig, msp, true);
        mig.migrate(true, true, true);

        GraknInserter gi = new GraknInserter("localhost", "48555", asp, keyspaceName);
        testNumberOfResponsesEntities(gi, keyspaceName);
        testNumberOfResponsesRelations(gi, keyspaceName);
        testNumberOfResponsesRelationOfRelations(gi, keyspaceName);
    }

    public void testNumberOfResponsesEntities(GraknInserter gi, String keyspace) {
        GraknClient client = gi.getClient();
        GraknClient.Session session = client.session(keyspace);

        // query person by phone-number
        GraknClient.Transaction read = session.transaction().read();
        GraqlGet getQuery = Graql.match(var("p").isa("person").has("phone-number", "+261 860 539 4754")).get().limit(1000);
        Assert.assertEquals(1, read.stream(getQuery).get().count());
        read.close();

        // query person by last name
        read = session.transaction().read();
        getQuery = Graql.match(var("p").isa("person").has("last-name", "Smith")).get().limit(1000);
        Assert.assertEquals(2, read.stream(getQuery).get().count());
        read.close();

        // query all entities of type person
        read = session.transaction().read();
        getQuery = Graql.match(var("c").isa("person")).get().limit(1000);
        Assert.assertEquals(32, read.stream(getQuery).get().count());
        read.close();

        // query all entites of type company
        read = session.transaction().read();
        getQuery = Graql.match(var("e").isa("company")).get().limit(1000);
        Assert.assertEquals(2, read.stream(getQuery).get().count());
        read.close();
    }

    public void testNumberOfResponsesRelations(GraknInserter gi, String keyspace) {
        GraknClient client = gi.getClient();
        GraknClient.Session session = client.session(keyspace);

        // query call by duration
        GraknClient.Transaction read = session.transaction().read();
        GraqlGet getQuery = Graql.match(var("c").isa("call").has("duration", 2851)).get().limit(1000);
        Assert.assertEquals(1, read.stream(getQuery).get().count());
        read.close();

        // query call by date
        read = session.transaction().read();
        DateTimeFormatter isoDateFormatter = DateTimeFormatter.ISO_DATE;
        String[] dt = "2018-09-17T18:43:42".split("T");
        LocalDate date = LocalDate.parse(dt[0], isoDateFormatter);
        if (dt.length > 1) {
            LocalTime time = LocalTime.parse(dt[1], DateTimeFormatter.ISO_TIME);
            LocalDateTime dateTime = date.atTime(time);
            getQuery = Graql.match(var("c").isa("call").has("started-at", dateTime)).get().limit(1000);
        } else {
            LocalDateTime dateTime = date.atStartOfDay();
            getQuery = Graql.match(var("c").isa("call").has("started-at", dateTime)).get().limit(1000);
        }
        Assert.assertEquals(1, read.stream(getQuery).get().count());
        read.close();

        // query call by caller
        read = session.transaction().read();
        StatementInstance player = Graql.var("p").isa("person").has("phone-number", "+7 171 898 0853");
        StatementInstance relation = Graql.var("c").isa("call").rel("caller", "p");
        ArrayList<StatementInstance> statements = new ArrayList<>();
        statements.add(player);
        statements.add(relation);
        getQuery = Graql.match(statements).get().limit(1000);
        Assert.assertEquals(14, read.stream(getQuery).get().count());
        read.close();

        // query call by callee
        read = session.transaction().read();
        player = Graql.var("p").isa("person").has("phone-number", "+7 171 898 0853");
        relation = Graql.var("c").isa("call").rel("callee", "p");
        statements = new ArrayList<>();
        statements.add(player);
        statements.add(relation);
        getQuery = Graql.match(statements).get().limit(1000);
        Assert.assertEquals(4, read.stream(getQuery).get().count());
        read.close();

        // query call by caller & callee
        read = session.transaction().read();
        StatementInstance playerOne = Graql.var("p1").isa("person").has("phone-number", "+7 171 898 0853");
        StatementInstance playerTwo = Graql.var("p2").isa("person").has("phone-number", "+57 629 420 5680");
        relation = Graql.var("c").isa("call").rel("caller", "p1").rel("callee", "p2");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = Graql.match(statements).get().limit(1000);
        Assert.assertEquals(4, read.stream(getQuery).get().count());
        read.close();
    }

    public void testNumberOfResponsesRelationOfRelations(GraknInserter gi, String keyspace) {
        GraknClient client = gi.getClient();
        GraknClient.Session session = client.session(keyspace);

        // query specific communication-channel and count the number of past calls (single past-call):
        GraknClient.Transaction read = session.transaction().read();
        StatementInstance playerOne = Graql.var("p1").isa("person").has("phone-number", "+54 398 559 0423");
        StatementInstance playerTwo = Graql.var("p2").isa("person").has("phone-number", "+48 195 624 2025");
        StatementInstance relation = Graql.var("c").isa("communication-channel").rel("peer", "p1").rel("peer", "p2").rel("past-call","x");
        ArrayList<Statement> statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        GraqlGet getQuery = Graql.match(statements).get("c", "x").limit(1000);
        Assert.assertEquals(1, read.stream(getQuery).get().count());
        getQuery = Graql.match(statements).get("x").limit(1000);
        Assert.assertEquals(1, read.stream(getQuery).get().count());
        read.close();

        // query specific communication-channel and count the number of past calls (listSeparated past-calls:
        read = session.transaction().read();
        playerOne = Graql.var("p1").isa("person").has("phone-number", "+263 498 495 0617");
        playerTwo = Graql.var("p2").isa("person").has("phone-number", "+33 614 339 0298");
        relation = Graql.var("c").isa("communication-channel").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(1, read.stream(getQuery).get().count());
        getQuery = Graql.match(statements).get("x").limit(1000);
        Assert.assertEquals(6, read.stream(getQuery).get().count());

        // make sure that this doesn't get inserted:
        read = session.transaction().read();
        playerOne = Graql.var("p1").isa("person").has("phone-number", "+7 690 597 4443");
        playerTwo = Graql.var("p2").isa("person").has("phone-number", "+54 398 559 9999");
        relation = Graql.var("c").isa("communication-channel").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(0, read.stream(getQuery).get().count());
        getQuery = Graql.match(statements).get("x").limit(1000);
        Assert.assertEquals(0, read.stream(getQuery).get().count());

        // these are added by doing player matching for past calls:
        // make sure that this doesn't get inserted:
        read = session.transaction().read();
        playerOne = Graql.var("p1").isa("person").has("phone-number", "+81 308 988 7153");
        playerTwo = Graql.var("p2").isa("person").has("phone-number", "+351 515 605 7915");
        relation = Graql.var("c").isa("communication-channel").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(5, read.stream(getQuery).get().count());
        getQuery = Graql.match(statements).get("x").limit(1000);
        Assert.assertEquals(5, read.stream(getQuery).get().count());

        read = session.transaction().read();
        playerOne = Graql.var("p1").isa("person").has("phone-number", "+7 171 898 0853");
        playerTwo = Graql.var("p2").isa("person").has("phone-number", "+57 629 420 5680");
        relation = Graql.var("c").isa("communication-channel").rel("peer", "p1").rel("peer", "p2").rel("past-call", "x");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(playerTwo);
        statements.add(relation);
        getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(4, read.stream(getQuery).get().count());
        getQuery = Graql.match(statements).get("x").limit(1000);
        Assert.assertEquals(4, read.stream(getQuery).get().count());

        // these must not be found (come from player-matched past-call):
        read = session.transaction().read();
        playerOne = Graql.var("p1").isa("person").has("phone-number", "+261 860 539 4754");
        relation = Graql.var("c").isa("communication-channel").rel("peer", "p1").rel("past-call", "x");
        statements = new ArrayList<>();
        statements.add(playerOne);
        statements.add(relation);
        getQuery = Graql.match(statements).get("c").limit(1000);
        Assert.assertEquals(0, read.stream(getQuery).get().count());
        getQuery = Graql.match(statements).get("x").limit(1000);
        Assert.assertEquals(0, read.stream(getQuery).get().count());
    }

    @Test
    public void issue10Test() throws IOException {

        String keyspaceName = "issue10";
        String asp = getAbsPath("src/test/resources/bugfixing/issue10/schema.gql");
        String msp = getAbsPath("src/test/resources/bugfixing/issue10/migrationStatus.json");
        String adcp = getAbsPath("src/test/resources/bugfixing/issue10/dataConfig.json");
        String gcp = getAbsPath("src/test/resources/bugfixing/issue10/processorConfig.json");

        MigrationConfig migrationConfig = new MigrationConfig("localhost:48555",keyspaceName, asp, adcp, gcp);
        GraknMigrator mig = new GraknMigrator(migrationConfig, msp, true);
        mig.migrate(true, true, true);

    }
}

