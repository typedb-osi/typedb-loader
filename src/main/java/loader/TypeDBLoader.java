package loader;

import config.Configuration;
import config.ConfigurationValidation;
import io.FileLogger;
import util.TypeDBUtil;
import util.Util;
import com.vaticle.typedb.client.TypeDB;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;

public class TypeDBLoader {

    private final Configuration dc;
    private final String databaseName;
    private final String typeDBURI;

    public TypeDBLoader(String dcPath,
                        String databaseName,
                        String typeDBURI) {
        this.dc = Util.initializeDataConfig(dcPath);
        this.databaseName = databaseName;
        this.typeDBURI = typeDBURI;
    }

    public void load() {

        TypeDBClient schemaClient = TypeDBUtil.getClient(typeDBURI);
        ConfigurationValidation cv = new ConfigurationValidation(dc);

        HashMap<String, ArrayList<String>> validationReport = new HashMap<>();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> warnings = new ArrayList<>();
        validationReport.put("warnings", warnings);
        validationReport.put("errors", errors);
        cv.validateSchemaPresent(validationReport);

        if (validationReport.get("errors").size() == 0) {
            TypeDBUtil.cleanAndDefineSchemaToDatabase(schemaClient, databaseName, dc.getDefaultConfig().getSchemaPath());
            Util.info("cleaned database and migrated schema...");
            FileLogger.getLogger().logToLoadingSummary("cleaned database and migrated schema...");
        }

        TypeDBSession schemaSession = TypeDBUtil.getSchemaSession(schemaClient, databaseName);
        cv.validateConfiguration(validationReport, schemaSession);

        if (validationReport.get("warnings").size() > 0) {
            validationReport.get("warnings").forEach(Util::warn);
            validationReport.get("warnings").forEach(FileLogger.getLogger()::logToLoadingSummary);
        }
        if (validationReport.get("errors").size() > 0) {
            validationReport.get("errors").forEach(Util::error);
            validationReport.get("errors").forEach(FileLogger.getLogger()::logToLoadingSummary);
            schemaSession.close();
            schemaClient.close();
            System.exit(1);
        }
        schemaSession.close();
        schemaClient.close();

        Instant start = Instant.now();
        try {
            AsyncLoaderWorker asyncLoaderWorker = null;
            try (TypeDBClient client = TypeDB.coreClient(typeDBURI, Runtime.getRuntime().availableProcessors())) {
                Runtime.getRuntime().addShutdownHook(
                        NamedThreadFactory.create(AsyncLoaderWorker.class, "shutdown").newThread(client::close)
                );
                asyncLoaderWorker = new AsyncLoaderWorker(dc, databaseName);
                asyncLoaderWorker.run(client);
            } finally {
                if (asyncLoaderWorker != null) asyncLoaderWorker.executor.shutdown();
            }
        } catch (Throwable e) {
            Util.error(e.getMessage(), e);
            Util.error("TERMINATED WITH ERROR");
            FileLogger.getLogger().logToLoadingSummary(String.format(e.getMessage().replace("{}", "%s"), e));
            FileLogger.getLogger().logToLoadingSummary("TERMINATED WITH ERROR");
        } finally {
            Instant end = Instant.now();
            Util.info("TypeDB Loader finished in: {}", Util.printDuration(start, end));
            FileLogger.getLogger().logToLoadingSummary(String.format("TypeDB Loader finished in: %s", Util.printDuration(start, end)));
        }
    }

}
