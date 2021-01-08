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

    @CommandLine.Option(names = {"-cm", "--cleanMigration"}, description = "optional - delete old schema and data and restart migration from scratch - default: continue previous migration, if exists")
    private boolean cleanMigration;

    @CommandLine.Option(names = {"-sc", "--scope"}, description = "optional - set migration scope: 0 - apply schema only (Note: this has no effect unless you also set the cleanMigration flag to true. Non-breaking schema updates (without previous clean) are coming in GraMi Version 0.0.3); 1 - apply schema and migrate entities; 2 - apply schema, migrate entities and relations; everything else defaults to 3 - apply schema and migrate all")
    private int scope = 3;

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
        spec.commandLine().getOut().println("\tusing data configuration: " + dataConfigFilePath);
        spec.commandLine().getOut().println("\tusing processor configuration: " + processorConfigFilePath);
        spec.commandLine().getOut().println("\ttracking migration status in: " + migrationStatusFilePath);
        spec.commandLine().getOut().println("\tusing schema: " + schemaName);
        spec.commandLine().getOut().println("\tmigrating to keyspace: " + keyspaceName);
        spec.commandLine().getOut().println("\tconnecting to grakn: " + graknURI);
        spec.commandLine().getOut().println("\tdelete keyspace and all data in it for a clean new migration?: " + cleanMigration);
        spec.commandLine().getOut().println("\tmigration scope: " + scope);

        final MigrationConfig migrationConfig = new MigrationConfig(graknURI, keyspaceName, schemaName, dataConfigFilePath, processorConfigFilePath);
        GraknMigrator mig = new GraknMigrator(migrationConfig, migrationStatusFilePath, cleanMigration);

        if (scope != 0 && scope != 1 && scope != 2) {
            scope = 3;
        }

        switch (scope) {
            case 0: mig.migrate(false, false, false); break;
            case 1: mig.migrate(true, false, false); break;
            case 2: mig.migrate(true, true, false); break;
            case 3: mig.migrate(true, true, true); break;
        }

        return 0;
    }
}
