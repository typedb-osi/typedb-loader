package a_rewrite.loader;

import a_rewrite.config.Configuration;
import a_rewrite.config.ConfigurationValidation;
import a_rewrite.util.TypeDBUtil;
import a_rewrite.util.Util;
import com.vaticle.typedb.client.TypeDB;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;

public class TypeDBLoader {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private final Configuration dc;
    private final String databaseName;
    private final String graknURI;

    public TypeDBLoader(String dcPath,
                        String databaseName,
                        String graknURI) {
        this.dc = Util.initializeDataConfig(dcPath);
        this.databaseName = databaseName;
        this.graknURI = graknURI;
    }

    public void load() {

        TypeDBClient schemaClient = TypeDBUtil.getClient(graknURI);
        ConfigurationValidation cv = new ConfigurationValidation(dc);

        HashMap<String, ArrayList<String>> validationReport = new HashMap<>();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> warnings = new ArrayList<>();
        validationReport.put("warnings", warnings);
        validationReport.put("errors", errors);
        cv.validateSchemaPresent(validationReport);

        if (validationReport.get("errors").size() == 0) {
            TypeDBUtil.cleanAndDefineSchemaToDatabase(schemaClient, databaseName, dc.getDefaultConfig().getSchemaPath());
            appLogger.info("cleaned database and migrated schema...");
        }

        TypeDBSession schemaSession = TypeDBUtil.getSchemaSession(schemaClient, databaseName);
        cv.validateConfiguration(validationReport, schemaSession);

        if (validationReport.get("warnings").size() > 0) {
            validationReport.get("warnings").forEach(appLogger::warn);
        }
        if (validationReport.get("errors").size() > 0) {
            validationReport.get("errors").forEach(appLogger::error);
            schemaSession.close();
            schemaClient.close();
            System.exit(1);
        }
        schemaSession.close();
        schemaClient.close();

        Instant start = Instant.now();
        try {
            AsyncLoaderWorker asyncLoaderWorker = null;
            try (TypeDBClient client = TypeDB.coreClient(graknURI, Runtime.getRuntime().availableProcessors())) {
                Runtime.getRuntime().addShutdownHook(
                        NamedThreadFactory.create(AsyncLoaderWorker.class, "shutdown").newThread(client::close)
                );
                asyncLoaderWorker = new AsyncLoaderWorker(dc, databaseName);
                asyncLoaderWorker.run(client);
            } finally {
                if (asyncLoaderWorker != null) asyncLoaderWorker.executor.shutdown();
            }
        } catch (Throwable e) {
            appLogger.error(e.getMessage(), e);
            appLogger.error("TERMINATED WITH ERROR");
        } finally {
            Instant end = Instant.now();
            appLogger.info("TypeDB Loader finished in: {}", Util.printDuration(start, end));
        }
    }

}
