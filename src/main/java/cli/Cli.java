package cli;

import configuration.MigrationConfig;
import migrator.GraknMigrator;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(description="Welcome to the CLI of GraMi - your grakn data migration tool", name = "GraMi", mixinStandardHelpOptions = true, version = "0.1")
public class Cli implements Callable<Integer> {
    @CommandLine.Spec CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-d", "--dataConfigFile"}, description = "data config file in JSON format", required = true)
    private String dataConfigFilePath;

    @CommandLine.Option(names = {"-p", "--processorConfigFile"}, description = "processor config file in JSON format", required = true)
    private String processorConfigFilePath;

    @CommandLine.Option(names = {"-m", "--migrationStatusFile"}, description = "file to track migration status in", required = true)
    private String migrationStatusFilePath;

    @CommandLine.Option(names = {"-s", "--schemaFile"}, description = "your schema file as .gql", required = true)
    private String schemaName;

    @CommandLine.Option(names = {"-k", "--keyspace"}, description = "target keyspace in your grakn instance", required = true)
    private String keyspaceName;

    @CommandLine.Option(names = {"-g", "--grakn"}, description = "optional - grakn DB in format: server:port (default: localhost:48555)", defaultValue = "localhost:48555")
    private String graknURI;

    @CommandLine.Option(names = {"-cf", "--cleanFirst"}, description = "optional - delete current schema and all data before loading schema and migrating entities/relations; default: continue migration using current keyspace schema")
    private boolean cleanFirst;

    @CommandLine.Option(names = {"-so", "--schemaOnly"}, description = "optional - only load schema and skip migration of entities/relations; default: load schema and migrate")
    private boolean schemaOnly;

    @CommandLine.Option(names = {"-eo", "--entitiesOnly"}, description = "optional - migrate entities only; default: migrate entities and relations")
    private boolean entitiesOnly;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Cli()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        spec.commandLine().getOut().println("##############################################");
        spec.commandLine().getOut().println("############## WELCOME TO GRAMI ##############");
        spec.commandLine().getOut().println("##############################################");
        spec.commandLine().getOut().println("migration started using the following parameters:");
        spec.commandLine().getOut().println("\tdata configuration file path: " + dataConfigFilePath);
        spec.commandLine().getOut().println("\tprocessor configuration file path: " + processorConfigFilePath);
        spec.commandLine().getOut().println("\tmigration status file path: " + migrationStatusFilePath);
        spec.commandLine().getOut().println("\tschema file path: " + schemaName);
        spec.commandLine().getOut().println("\tkeyspace name: " + keyspaceName);
        spec.commandLine().getOut().println("\tgrakn uri: " + graknURI);
        spec.commandLine().getOut().println("\tclean keyspace first?: " + cleanFirst);
        spec.commandLine().getOut().println("\tmigrate schema only?: " + schemaOnly);
        spec.commandLine().getOut().println("\tmigrate entities only?: " + entitiesOnly);

        final MigrationConfig migrationConfig = new MigrationConfig(graknURI, keyspaceName, schemaName, dataConfigFilePath, processorConfigFilePath);

        // ensure that user cannot set flags so that relations are attempted to be migrated without migrating entities first
        if (schemaOnly) {
            entitiesOnly = true;
        }

        GraknMigrator mig = new GraknMigrator(migrationConfig, migrationStatusFilePath, cleanFirst);
        mig.migrate(!schemaOnly, !entitiesOnly);
        return 0;
    }
}
