package migrator;

import configuration.MigrationConfig;
import configuration.SchemaUpdateConfig;
import grakn.client.GraknClient;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.statement.Statement;
import graql.lang.statement.StatementInstance;
import insert.GraknInserter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static util.Util.getAbsPath;

public class SchemaUpdaterTest {

    @Test
    public void updateSchemaTest() throws IOException {

        String keyspaceName = "grami_phone_call_test_su";
        String asp = getAbsPath("src/test/resources/phone-calls/schema.gql");
        String msp = getAbsPath("src/test/resources/phone-calls/migrationStatus.json");
        String adcp = getAbsPath("src/test/resources/phone-calls/dataConfig.json");
        String gcp = getAbsPath("src/test/resources/phone-calls/processorConfig.json");

        MigrationConfig migrationConfig = new MigrationConfig("localhost:48555",keyspaceName, asp, adcp, gcp);
        GraknMigrator mig = new GraknMigrator(migrationConfig, msp, true);
        mig.migrate(true, true, true,true);

        GraknInserter gi = new GraknInserter("localhost", "48555", asp, keyspaceName);

        asp = getAbsPath("src/test/resources/phone-calls/schema-updated.gql");
        SchemaUpdateConfig suConfig = new SchemaUpdateConfig("localhost:48555",keyspaceName, asp);
        SchemaUpdater su = new SchemaUpdater(suConfig);
        su.updateSchema();

        postUpdateSchemaTests(gi, keyspaceName);
    }

    private void postUpdateSchemaTests(GraknInserter gi, String keyspace) {
        GraknClient client = gi.getClient();
        GraknClient.Session session = client.session(keyspace);

        // query attribute type
        GraknClient.Transaction read = session.transaction().read();
        GraqlGet getQuery = Graql.match(var("a").type("added-attribute")).get().limit(1000);
        Assert.assertEquals(1, read.stream(getQuery).get().count());
        read.close();

        // query entity type
        read = session.transaction().read();
        getQuery = Graql.match(var("a").type("added-entity")).get().limit(1000);
        Assert.assertEquals(1, read.stream(getQuery).get().count());
        read.close();

        // query relation type
        read = session.transaction().read();
        getQuery = Graql.match(var("a").type("added-relation")).get().limit(1000);
        Assert.assertEquals(1, read.stream(getQuery).get().count());
        read.close();

        // query role type
        read = session.transaction().read();
        getQuery = Graql.match(type("added-relation").relates(var("r"))).get().limit(1000);
        Assert.assertEquals(1, read.stream(getQuery).get().count());
        read.close();
    }

}

