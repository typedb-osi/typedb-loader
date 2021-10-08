package cli;

import picocli.CommandLine;

@CommandLine.Command(name = "load", description = "run a migration", mixinStandardHelpOptions = true)
public class LoadOptions {
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-c", "--config"}, description = "config file in JSON format", required = true)
    public String dataConfigFilePath;

    @CommandLine.Option(names = {"-db", "--database"}, description = "target database in your grakn instance", required = true)
    public String databaseName;

    @CommandLine.Option(names = {"-tdb", "--typedb"}, description = "optional - TypeDB server in format: server:port (default: localhost:1729)", defaultValue = "localhost:1729")
    public String typedbURI;

    @CommandLine.Option(names = {"-cm", "--cleanMigration"}, description = "optional - delete old schema and data and restart migration from scratch - default: continue previous migration, if exists", defaultValue = "false")
    public boolean cleanMigration;

    @CommandLine.Option(names = {"-ls", "--loadSchema"}, description = "optional - reload schema when continuing a migration (ignored when clean migration)", defaultValue = "false")
    public boolean loadSchema;

    public static LoadOptions parse(String[] args) {
        CommandLine commandLine = new CommandLine(new TypeDBLoaderCLI())
                .addSubcommand("load", new LoadOptions());
        CommandLine.ParseResult arguments = commandLine.parseArgs(args);
        assert arguments.subcommands().size() == 1;
        return arguments.subcommand().asCommandLineList().get(0).getCommand();
    }

    public void print() {
        spec.commandLine().getOut().println("############## TypeDB Loader ###############");
        spec.commandLine().getOut().println("TypeDB Loader started with parameters:");
        spec.commandLine().getOut().println("\tconfiguration: " + dataConfigFilePath);
        spec.commandLine().getOut().println("\tdatabase name: " + databaseName);
        spec.commandLine().getOut().println("\tTypeDB server: " + typedbURI);
        spec.commandLine().getOut().println("\tdelete database and all data in it for a clean new migration?: " + cleanMigration);
        spec.commandLine().getOut().println("\tdo not load schema?: " + loadSchema);
    }
}