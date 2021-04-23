package cli;

import configuration.MigrationConfig;
import configuration.SchemaUpdateConfig;
import migrator.GraknMigrator;
import schema.SchemaUpdater;
import picocli.CommandLine;

import java.io.IOException;

@CommandLine.Command(description="Welcome to the CLI of GraMi - your grakn data migration tool", name = "grami", version = "0.1.1", mixinStandardHelpOptions = true)
public class GramiCLI {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new GramiCLI())
                .addSubcommand("migrate", new MigrateCommand())
                .addSubcommand("update", new SchemaUpdateCommand())
                .execute(args);
        System.exit(exitCode);
    }

}

@CommandLine.Command(name = "migrate", description = "run a migration", mixinStandardHelpOptions = true)
class MigrateCommand implements Runnable {
    @CommandLine.Spec CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-dc", "--dataConfigFile"}, description = "data config file in JSON format", required = true)
    private String dataConfigFilePath;

    @CommandLine.Option(names = {"-pc", "--processorConfigFile"}, description = "processor config file in JSON format", required = true)
    private String processorConfigFilePath;

    @CommandLine.Option(names = {"-ms", "--migrationStatusFile"}, description = "file to track migration status in", required = true)
    private String migrationStatusFilePath;

    @CommandLine.Option(names = {"-s", "--schemaFile"}, description = "your schema file as .gql", required = true)
    private String schemaFilePath;

    @CommandLine.Option(names = {"-db", "--database"}, description = "target database in your grakn instance", required = true)
    private String databaseName;

    @CommandLine.Option(names = {"-g", "--grakn"}, description = "optional - grakn DB in format: server:port (default: localhost:1729)", defaultValue = "localhost:1729")
    private String graknURI;

    @CommandLine.Option(names = {"-cm", "--cleanMigration"}, description = "optional - delete old schema and data and restart migration from scratch - default: continue previous migration, if exists")
    private boolean cleanMigration;

    @Override
    public void run() {
        spec.commandLine().getOut().println("############## GraMi migration ###############");
        spec.commandLine().getOut().println("migration started with parameters:");
        spec.commandLine().getOut().println("\tdata configuration: " + dataConfigFilePath);
        spec.commandLine().getOut().println("\tprocessor configuration: " + processorConfigFilePath);
        spec.commandLine().getOut().println("\ttracking migration status in: " + migrationStatusFilePath);
        spec.commandLine().getOut().println("\tschema: " + schemaFilePath);
        spec.commandLine().getOut().println("\tdatabase: " + databaseName);
        spec.commandLine().getOut().println("\tgrakn server: " + graknURI);
        spec.commandLine().getOut().println("\tdelete database and all data in it for a clean new migration?: " + cleanMigration);

        final MigrationConfig migrationConfig = new MigrationConfig(graknURI, databaseName, schemaFilePath, dataConfigFilePath, processorConfigFilePath);

        try {
            GraknMigrator mig = new GraknMigrator(migrationConfig, migrationStatusFilePath, cleanMigration);
            mig.migrate();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

@CommandLine.Command(name = "schema-update", description = "update a schema using a .gql file", mixinStandardHelpOptions = true)
class SchemaUpdateCommand implements Runnable {
    @CommandLine.Spec CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-s", "--schemaFile"}, description = "your schema file as .gql", required = true)
    private String schemaFilePath;

    @CommandLine.Option(names = {"-db", "--database"}, description = "target database in your grakn instance", required = true)
    private String databaseName;

    @CommandLine.Option(names = {"-g", "--grakn"}, description = "optional - grakn DB in format: server:port (default: localhost:1729)", defaultValue = "localhost:1729")
    private String graknURI;

    @Override
    public void run() {
        spec.commandLine().getOut().println("############## GraMi schema-update ###############");
        spec.commandLine().getOut().println("schema-update started with parameters:");
        spec.commandLine().getOut().println("\tschema: " + schemaFilePath);
        spec.commandLine().getOut().println("\tkeyspace: " + databaseName);
        spec.commandLine().getOut().println("\tgrakn server: " + graknURI);

        SchemaUpdateConfig suConfig = new SchemaUpdateConfig(graknURI, databaseName, schemaFilePath);
        SchemaUpdater su = new SchemaUpdater(suConfig);
        su.updateSchema();
    }
}
