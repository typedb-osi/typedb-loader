package cli;

import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typeql.lang.TypeQL;
import org.junit.Test;
import picocli.CommandLine;
import util.TypeDBUtil;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TypeDBLoaderCLITest {
    @Test
    public void migrateTest() {
        String[] args = {
                "load",
                "-c", "src/test/resources/1.0.0/phoneCalls/dc.json",
                "-db", "typedb_loader_cli_test",
                "-tdb", "127.0.0.1:1729",
                "-cm"
        };

        TypeDBLoaderCLI loader = new TypeDBLoaderCLI();
        LoadCommand load = new LoadCommand();

        CommandLine cli = new CommandLine(loader).addSubcommand(load);

        StringWriter sw = new StringWriter();
        cli.setOut(new PrintWriter(sw));

        int exitCode = cli.execute(args);
        assertEquals(0, exitCode);
        assertTrue(sw.toString().contains("############## TypeDB Loader ###############"));
        assertTrue(sw.toString().contains("TypeDB Loader started with parameters:"));
        assertTrue(sw.toString().contains("configuration: src/test/resources/1.0.0/phoneCalls/dc.json"));
        assertTrue(sw.toString().contains("database name: typedb_loader_cli_test"));
        assertTrue(sw.toString().contains("TypeDB server: 127.0.0.1:1729"));
        assertTrue(sw.toString().contains("delete database and all data in it for a clean new migration?: true"));
    }

    @Test
    public void migrateTestContinue() {
        String db = "typedb_loader_cli_test";
        String uri = "127.0.0.1:1729";
        String[] argsCleanMigration ={
                "load",
                "-c", "src/test/resources/1.0.0/phoneCalls/dc.json",
                "-db", db,
                "-tdb", uri,
                "-cm"
        };

        String[] args = {
                "load",
                "-c", "src/test/resources/1.0.0/phoneCalls/dc.json",
                "-db", db,
                "-tdb", uri
        };

        TypeDBLoaderCLI loader = new TypeDBLoaderCLI();
        LoadCommand load = new LoadCommand();

        CommandLine cli = new CommandLine(loader).addSubcommand(load);

        StringWriter sw = new StringWriter();
        cli.setOut(new PrintWriter(sw));

        // run import once
        cli.execute(argsCleanMigration);
        // delete all the data
        clearData(uri, db);
        int exitCode = cli.execute(args);
        assertEquals(0, exitCode);
        assertTrue(sw.toString().contains("############## TypeDB Loader ###############"));
        assertTrue(sw.toString().contains("TypeDB Loader started with parameters:"));
        assertTrue(sw.toString().contains("configuration: src/test/resources/1.0.0/phoneCalls/dc.json"));
        assertTrue(sw.toString().contains("database name: typedb_loader_cli_test"));
        assertTrue(sw.toString().contains("TypeDB server: 127.0.0.1:1729"));
        assertTrue(sw.toString().contains("delete database and all data in it for a clean new migration?: false"));
    }

    private void clearData(String uri, String db) {
        System.out.println("Cleaning all previous loaded data in: " + db);
        TypeDBClient client = TypeDBUtil.getClient(uri);
        try (TypeDBSession session = TypeDBUtil.getDataSession(client, db)) {
            try (TypeDBTransaction txn = session.transaction(TypeDBTransaction.Type.WRITE)) {
                txn.query().delete(TypeQL.parseQuery("match $x isa thing; delete $x isa thing;").asDelete());
                txn.commit();
            }
        }
    }
}
