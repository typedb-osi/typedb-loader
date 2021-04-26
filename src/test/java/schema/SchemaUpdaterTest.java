package schema;

import configuration.MigrationConfig;
import configuration.SchemaUpdateConfig;
import grakn.client.api.GraknClient;
import grakn.client.api.GraknSession;
import grakn.client.api.GraknTransaction;
import graql.lang.Graql;
import graql.lang.query.GraqlMatch;
import write.TypeDBWriter;
import loader.TypeDBLoader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static graql.lang.Graql.type;
import static graql.lang.Graql.var;
import static util.Util.getAbsPath;

public class SchemaUpdaterTest {

    String graknURI = "localhost:1729";

    @Test
    public void updateSchemaTest() throws IOException {

        String databaseName = "grami_phone_call_test_su";
        String asp = getAbsPath("src/test/resources/phoneCalls/schema.gql");
        String msp = getAbsPath("src/test/resources/phoneCalls/migrationStatus.json");
        String adcp = getAbsPath("src/test/resources/phoneCalls/dataConfig.json");
        String gcp = getAbsPath("src/test/resources/phoneCalls/processorConfig.json");

        MigrationConfig migrationConfig = new MigrationConfig(graknURI,databaseName, asp, adcp, gcp);
        TypeDBLoader mig = new TypeDBLoader(migrationConfig, msp, true);
        mig.migrate();

        TypeDBWriter gi = new TypeDBWriter(graknURI.split(":")[0], graknURI.split(":")[1], asp, databaseName);

        asp = getAbsPath("src/test/resources/phoneCalls/schema-updated.gql");
        SchemaUpdateConfig suConfig = new SchemaUpdateConfig(graknURI,databaseName, asp);
        SchemaUpdater su = new SchemaUpdater(suConfig);
        su.updateSchema();

        postUpdateSchemaTests(gi);
    }

    private void postUpdateSchemaTests(TypeDBWriter gi) {
        GraknClient client = gi.getClient();
        GraknSession session = gi.getDataSession(client);

        // query attribute type
        GraknTransaction read = session.transaction(GraknTransaction.Type.READ);
        GraqlMatch.Filtered getQuery = Graql.match(var("a").type("added-attribute")).get("a");
        Assert.assertEquals(1, read.query().match(getQuery).count());
        read.close();

        // query entity type
        read = session.transaction(GraknTransaction.Type.READ);
        GraqlMatch.Limited getQuery2 = Graql.match(var("a").type("added-entity")).get("a").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery2).count());
        read.close();

        // query relation type
        read = session.transaction(GraknTransaction.Type.READ);
        getQuery2 = Graql.match(var("a").type("added-relation")).get("a").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery2).count());
        read.close();

        // query role type
        read = session.transaction(GraknTransaction.Type.READ);
        getQuery2 = Graql.match(type("added-relation").relates(var("r"))).limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery2).count());
        read.close();

        session.close();
        client.close();
    }

}

