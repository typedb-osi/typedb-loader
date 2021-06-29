package a_rewrite.loader;

import a_rewrite.util.GraknUtil;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

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

        TypeDBClient client = GraknUtil.getClient(graknUri, 4);
        TypeDBSession session = GraknUtil.getDataSession(client, databaseName);

        testAttributes(session);
        testEntities(session);

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
}
