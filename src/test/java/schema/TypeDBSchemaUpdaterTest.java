package schema;

import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import configuration.LoaderLoadConfig;
import configuration.LoaderSchemaUpdateConfig;
import write.TypeDBWriter;
import loader.TypeDBLoader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import static util.Util.getAbsPath;

public class TypeDBSchemaUpdaterTest {

    String graknURI = "localhost:1729";

    @Test
    public void updateSchemaTest() throws IOException {

        String databaseName = "grami_phone_call_test_su";
        String asp = getAbsPath("src/test/resources/phoneCalls/schema.gql");
        String msp = getAbsPath("src/test/resources/phoneCalls/migrationStatus.json");
        String adcp = getAbsPath("src/test/resources/phoneCalls/dataConfig.json");
        String gcp = getAbsPath("src/test/resources/phoneCalls/processorConfig.json");

        LoaderLoadConfig loaderLoadConfig = new LoaderLoadConfig(graknURI,databaseName, asp, adcp, gcp);
        TypeDBLoader mig = new TypeDBLoader(loaderLoadConfig, msp, true);
        mig.migrate();

        TypeDBWriter gi = new TypeDBWriter(graknURI.split(":")[0], graknURI.split(":")[1], asp, databaseName);

        asp = getAbsPath("src/test/resources/phoneCalls/schema-updated.gql");
        LoaderSchemaUpdateConfig suConfig = new LoaderSchemaUpdateConfig(graknURI,databaseName, asp);
        TypeDBSchemaUpdater su = new TypeDBSchemaUpdater(suConfig);
        su.updateSchema();

        postUpdateSchemaTests(gi);
    }

    private void postUpdateSchemaTests(TypeDBWriter gi) {
        TypeDBClient client = gi.getClient();
        TypeDBSession session = gi.getDataSession(client);

        // query attribute type
        TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ);
        TypeQLMatch.Filtered getQuery = TypeQL.match(TypeQL.var("a").type("added-attribute")).get("a");
        Assert.assertEquals(1, read.query().match(getQuery).count());
        read.close();

        // query entity type
        read = session.transaction(TypeDBTransaction.Type.READ);
        TypeQLMatch.Limited getQuery2 = TypeQL.match(TypeQL.var("a").type("added-entity")).get("a").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery2).count());
        read.close();

        // query relation type
        read = session.transaction(TypeDBTransaction.Type.READ);
        getQuery2 = TypeQL.match(TypeQL.var("a").type("added-relation")).get("a").limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery2).count());
        read.close();

        // query role type
        read = session.transaction(TypeDBTransaction.Type.READ);
        getQuery2 = TypeQL.match(TypeQL.type("added-relation").relates(TypeQL.var("r"))).limit(1000);
        Assert.assertEquals(1, read.query().match(getQuery2).count());
        read.close();

        session.close();
        client.close();
    }

}

