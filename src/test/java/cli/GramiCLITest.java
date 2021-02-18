package cli;

import org.junit.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GramiCLITest {
    @Test
    public void migrateTest() {
        String[] args = {
                "migrate",
                "-d", "src/test/resources/phone-calls/dataConfig.json",
                "-p", "src/test/resources/phone-calls/processorConfig.json",
                "-m", "src/test/resources/phone-calls/migrationStatus.json",
                "-s", "src/test/resources/phone-calls/schema.gql",
                "-k", "grami_cli_test",
                "-g", "127.0.0.1:1729",
                "-cm"
        };

        GramiCLI grami = new GramiCLI();
        MigrateCommand migrate = new MigrateCommand();

        CommandLine cli = new CommandLine(grami).addSubcommand(migrate);

        StringWriter sw = new StringWriter();
        cli.setOut(new PrintWriter(sw));

        int exitCode = cli.execute(args);
        assertEquals(0, exitCode);
        assertTrue(sw.toString().contains("GraMi migration"));
    }

    @Test
    public void updateTest() {
        String[] args = {
                "schema-update",
                "-s", "src/test/resources/phone-calls/schema-updated.gql",
                "-k", "grami_cli_test",
                "-g", "127.0.0.1:1729",
        };

        GramiCLI grami = new GramiCLI();
        SchemaUpdateCommand schemaUpdateCommand = new SchemaUpdateCommand();
        CommandLine cli = new CommandLine(grami).addSubcommand(schemaUpdateCommand);

        StringWriter sw = new StringWriter();
        cli.setOut(new PrintWriter(sw));

        int exitCode = cli.execute(args);
        assertEquals(0, exitCode);
        assertTrue(sw.toString().contains("GraMi schema-update"));
        assertTrue(sw.toString().contains("schema-updated.gql"));
        assertTrue(sw.toString().contains("grami_cli_test"));
        assertTrue(sw.toString().contains("1729"));
    }
}
