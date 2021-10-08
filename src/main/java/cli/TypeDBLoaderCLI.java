package cli;

import loader.TypeDBLoader;
import picocli.CommandLine;

@CommandLine.Command(description = "Welcome to the CLI of TypeDB Loader - your TypeDB data loading tool", name = "TypeDB Loader", version = "1.0.0-alpha", mixinStandardHelpOptions = true)
public class TypeDBLoaderCLI {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new TypeDBLoaderCLI())
                .addSubcommand("load", new LoadCommand())
                .execute(args);
        System.exit(exitCode);
    }

}

@CommandLine.Command(name = "load", description = "run a migration", mixinStandardHelpOptions = true)
class LoadCommand implements Runnable {
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-c", "--config"}, description = "config file in JSON format", required = true)
    private String dataConfigFilePath;

    @CommandLine.Option(names = {"-db", "--database"}, description = "target database in your grakn instance", required = true)
    private String databaseName;

    @CommandLine.Option(names = {"-tdb", "--typedb"}, description = "optional - TypeDB server in format: server:port (default: localhost:1729)", defaultValue = "localhost:1729")
    private String typedbURI;

    @CommandLine.Option(names = {"-cm", "--cleanMigration"}, description = "optional - delete old schema and data and restart migration from scratch - default: continue previous migration, if exists")
    private boolean cleanMigration;

    @Override
    public void run() {
        spec.commandLine().getOut().println("############## TypeDB Loader ###############");
        spec.commandLine().getOut().println("TypeDB Loader started with parameters:");
        spec.commandLine().getOut().println("\tconfiguration: " + dataConfigFilePath);
        spec.commandLine().getOut().println("\tdatabase name: " + databaseName);
        spec.commandLine().getOut().println("\tTypeDB server: " + typedbURI);
        spec.commandLine().getOut().println("\tdelete database and all data in it for a clean new migration?: " + cleanMigration);

        TypeDBLoader loader = new TypeDBLoader(dataConfigFilePath, databaseName, typedbURI);
        loader.load();
    }
}
