package cli;

import configuration.LoaderLoadConfig;
import configuration.LoaderSchemaUpdateConfig;
import loader.TypeDBLoader;
import picocli.CommandLine;
import schema.TypeDBSchemaUpdater;

import java.io.IOException;

@CommandLine.Command(description = "Welcome to the CLI of TypeDB Loader - your TypeDB data loading tool", name = "TypeDB Loader", version = "0.1.2", mixinStandardHelpOptions = true)
public class TypeDBLoaderCLI {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new TypeDBLoaderCLI())
                .addSubcommand("load", new LoadCommand())
                .addSubcommand("schema-update", new SchemaUpdateCommand())
                .execute(args);
        System.exit(exitCode);
    }

}

@CommandLine.Command(name = "load", description = "run a migration", mixinStandardHelpOptions = true)
class LoadCommand implements Runnable {
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

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

    @CommandLine.Option(names = {"-tdb", "--typedb"}, description = "optional - TypeDB server in format: server:port (default: localhost:1729)", defaultValue = "localhost:1729")
    private String typedbURI;

    @CommandLine.Option(names = {"-cm", "--cleanMigration"}, description = "optional - delete old schema and data and restart migration from scratch - default: continue previous migration, if exists")
    private boolean cleanMigration;

    @Override
    public void run() {
        spec.commandLine().getOut().println("############## TypeDB Loader: migration ###############");
        spec.commandLine().getOut().println("migration started with parameters:");
        spec.commandLine().getOut().println("\tdata configuration: " + dataConfigFilePath);
        spec.commandLine().getOut().println("\tprocessor configuration: " + processorConfigFilePath);
        spec.commandLine().getOut().println("\ttracking migration status in: " + migrationStatusFilePath);
        spec.commandLine().getOut().println("\tschema: " + schemaFilePath);
        spec.commandLine().getOut().println("\tdatabase: " + databaseName);
        spec.commandLine().getOut().println("\tTypeDB server: " + typedbURI);
        spec.commandLine().getOut().println("\tdelete database and all data in it for a clean new migration?: " + cleanMigration);

        final LoaderLoadConfig loaderLoadConfig = new LoaderLoadConfig(typedbURI, databaseName, schemaFilePath, dataConfigFilePath, processorConfigFilePath);

        try {
            TypeDBLoader mig = new TypeDBLoader(loaderLoadConfig, migrationStatusFilePath, cleanMigration);
            mig.migrate();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

@CommandLine.Command(name = "schema-update", description = "update a schema using a .gql file", mixinStandardHelpOptions = true)
class SchemaUpdateCommand implements Runnable {
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-s", "--schemaFile"}, description = "your schema file as .gql", required = true)
    private String schemaFilePath;

    @CommandLine.Option(names = {"-db", "--database"}, description = "target database in your grakn instance", required = true)
    private String databaseName;

    @CommandLine.Option(names = {"-tdb", "--typedb"}, description = "optional - TypeDB server in format: server:port (default: localhost:1729)", defaultValue = "localhost:1729")
    private String typeDBURI;

    @Override
    public void run() {
        spec.commandLine().getOut().println("############## TypeDB Loader: schema-update ###############");
        spec.commandLine().getOut().println("schema-update started with parameters:");
        spec.commandLine().getOut().println("\tschema: " + schemaFilePath);
        spec.commandLine().getOut().println("\tdatabase: " + databaseName);
        spec.commandLine().getOut().println("\tTypeDB server: " + typeDBURI);

        LoaderSchemaUpdateConfig suConfig = new LoaderSchemaUpdateConfig(typeDBURI, databaseName, schemaFilePath);
        TypeDBSchemaUpdater su = new TypeDBSchemaUpdater(suConfig);
        su.updateSchema();
    }
}
