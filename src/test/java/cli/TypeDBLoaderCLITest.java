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
//        String[] args = {
//                "load",
//                "-c", "src/test/resources/phoneCalls/dataConfig.json",
//                "-db", "grami_cli_test",
//                "-tdb", "127.0.0.1:1729",
//                "-cm"
//        };
//
//        TypeDBLoaderCLI loader = new TypeDBLoaderCLI();
//        LoadCommand load = new LoadCommand();
//
//        CommandLine cli = new CommandLine(loader).addSubcommand(load);
//
//        StringWriter sw = new StringWriter();
//        cli.setOut(new PrintWriter(sw));
//
//        int exitCode = cli.execute(args);
//        assertEquals(0, exitCode);
//        assertTrue(sw.toString().contains("TypeDB Loader: migration"));
    }
}
