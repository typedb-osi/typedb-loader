package cli;

import org.junit.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TypeDBLoaderCLITest {
    @Test
    public void migrateTest() {
        String[] args = {
                "load",
                "-dc", "src/test/resources/phoneCalls/dataConfig.json",
                "-pc", "src/test/resources/phoneCalls/processorConfig.json",
                "-ms", "src/test/resources/phoneCalls/migrationStatus.json",
                "-s", "src/test/resources/phoneCalls/schema.gql",
                "-db", "grami_cli_test",
                "-tdb", "127.0.0.1:1729",
                "-cm"
        };

        TypeDBLoaderCLI loader = new TypeDBLoaderCLI();
        LoadCommand migrate = new LoadCommand();

        CommandLine cli = new CommandLine(loader).addSubcommand(migrate);

        StringWriter sw = new StringWriter();
        cli.setOut(new PrintWriter(sw));

        int exitCode = cli.execute(args);
        assertEquals(0, exitCode);
        assertTrue(sw.toString().contains("TypeDB Loader: migration"));
    }

    @Test
    public void updateTest() {
        String[] args = {
                "schema-update",
                "-s", "src/test/resources/phoneCalls/schema-updated.gql",
                "-db", "grami_cli_test",
                "-tdb", "127.0.0.1:1729",
        };

        TypeDBLoaderCLI loader = new TypeDBLoaderCLI();
        SchemaUpdateCommand schemaUpdateCommand = new SchemaUpdateCommand();
        CommandLine cli = new CommandLine(loader).addSubcommand(schemaUpdateCommand);

        StringWriter sw = new StringWriter();
        cli.setOut(new PrintWriter(sw));

        int exitCode = cli.execute(args);
        assertEquals(0, exitCode);
        assertTrue(sw.toString().contains("TypeDB Loader: schema-update"));
        assertTrue(sw.toString().contains("schema-updated.gql"));
        assertTrue(sw.toString().contains("grami_cli_test"));
        assertTrue(sw.toString().contains("1729"));
    }
}
