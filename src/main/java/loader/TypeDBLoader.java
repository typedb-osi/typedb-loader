package loader;

import cli.LoadOptions;
import com.vaticle.typedb.client.TypeDB;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import config.Configuration;
import config.ConfigurationValidation;
import util.TypeDBUtil;
import util.Util;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;

public class TypeDBLoader {

    private final LoadOptions options;
    private final Configuration dc;

    public TypeDBLoader(LoadOptions options) {
        this.options = options;
        this.dc = Util.initializeDataConfig(options.dataConfigFilePath);
    }

    public void load() {
        Util.info("validating your config...");
        TypeDBClient schemaClient = TypeDBUtil.getClient(options.typedbURI);
        ConfigurationValidation cv = new ConfigurationValidation(dc);
        HashMap<String, ArrayList<String>> validationReport = new HashMap<>();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> warnings = new ArrayList<>();
        validationReport.put("warnings", warnings);
        validationReport.put("errors", errors);
        cv.validateSchemaPresent(validationReport);

        if (validationReport.get("errors").size() == 0) {
            if (options.cleanMigration) {
                TypeDBUtil.cleanAndDefineSchemaToDatabase(schemaClient, options.databaseName, dc.getGlobalConfig().getSchemaPath());
                Util.info("cleaned database and migrated schema...");
            } else if (options.loadSchema) {
                TypeDBUtil.loadAndDefineSchema(schemaClient, options.databaseName, dc.getGlobalConfig().getSchemaPath());
                Util.info("loaded schema...");
            }
        }

        TypeDBSession schemaSession = TypeDBUtil.getSchemaSession(schemaClient, options.databaseName);
        cv.validateConfiguration(validationReport, schemaSession);

        if (validationReport.get("warnings").size() > 0) {
            validationReport.get("warnings").forEach(Util::warn);
        }
        if (validationReport.get("errors").size() > 0) {
            validationReport.get("errors").forEach(Util::error);
            schemaSession.close();
            schemaClient.close();
            System.exit(1);
        }
        schemaSession.close();
        schemaClient.close();
        Util.info("finished validating your config...");

        Instant start = Instant.now();
        try {
            AsyncLoaderWorker asyncLoaderWorker = null;
            try (TypeDBClient client = TypeDB.coreClient(options.typedbURI, Runtime.getRuntime().availableProcessors())) {
                Runtime.getRuntime().addShutdownHook(
                        NamedThreadFactory.create(AsyncLoaderWorker.class, "shutdown").newThread(client::close)
                );
                asyncLoaderWorker = new AsyncLoaderWorker(dc, options.databaseName);
                asyncLoaderWorker.run(client);
            } finally {
                if (asyncLoaderWorker != null) asyncLoaderWorker.executor.shutdown();
            }
        } catch (Throwable e) {
            Util.error(e.getMessage(), e);
            Util.error("TERMINATED WITH ERROR");
        } finally {
            Instant end = Instant.now();
            Util.info("TypeDB Loader finished in: {}", Util.printDuration(start, end));
        }
    }

}
