package loader;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import configuration.*;
import write.TypeDBWriter;
import io.FileToInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import processor.*;
import status.MigrationStatus;

import java.io.*;
import java.lang.reflect.Type;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;


public class TypeDBLoader {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private final HashMap<String, ConfigEntryData> dataConfig;
    private final String migrationStatePath;
    private final TypeDBWriter typeDBWriter;
    private final LoaderLoadConfig loaderLoadConfig;
    private boolean cleanAndMigrate = false;
    private HashMap<String, MigrationStatus> migrationStatus;

    public TypeDBLoader(LoaderLoadConfig loaderLoadConfig,
                        String migrationStatePath) {
        this.dataConfig = loaderLoadConfig.getDataConfig();
        this.migrationStatePath = migrationStatePath;
        this.loaderLoadConfig = loaderLoadConfig;
        this.typeDBWriter = new TypeDBWriter(loaderLoadConfig.getGraknURI().split(":")[0],
                loaderLoadConfig.getGraknURI().split(":")[1],
                loaderLoadConfig.getSchemaPath(),
                loaderLoadConfig.getKeyspace()
        );
    }

    public TypeDBLoader(LoaderLoadConfig loaderLoadConfig,
                        String migrationStatePath,
                        boolean cleanAndMigrate) throws IOException {
        this(loaderLoadConfig, migrationStatePath);
        if (cleanAndMigrate) {
            this.cleanAndMigrate = true;
            clearMigrationStatusFile();
        }
    }

    public void migrate() throws IOException {

        initializeMigrationStatus();
        TypeDBClient client = typeDBWriter.getClient();

        if (cleanAndMigrate) {
            typeDBWriter.cleanAndDefineSchemaToDatabase(client);
            appLogger.info("cleaned database and migrate schema...");
        } else {
            appLogger.info("using existing DB and schema to continue previous migration...");
        }

        HashMap<String, ArrayList<String>> validation = ConfigValidation.validateConfigs(loaderLoadConfig);
        if (validation.get("processorConfig").size() > 0 ||
                validation.get("processorConfig").size() > 0) {
            appLogger.error("Found errors in configuration files: ");
            for (String error : validation.get("processorConfig")) {
                appLogger.error(error);
            }
            for (String error : validation.get("dataConfig")) {
                appLogger.error(error);
            }
            appLogger.error("Aborting migration - please fix configuration errors");
            System.exit(1);
        }

        TypeDBSession dataSession = typeDBWriter.getDataSession(client);
        migrateThingsInOrder(dataSession);

        dataSession.close();
        client.close();
        appLogger.info("GraMi is finished migrating your stuff!");
    }

    private void migrateThingsInOrder(TypeDBSession session) throws IOException {
        List<EntryMigrationConfig> migrationBatch;

        // independent attributes
        appLogger.info("migrating independent attributes...");
        migrationBatch = getEntryMigrationConfigsByProcessorType(ProcessorType.ATTRIBUTE);
        for (EntryMigrationConfig conf : migrationBatch) {
            appLogger.info("starting migration for: [" + conf.getMigrationStatusKey() + "]");
            batchDataBuildQueriesAndInsert(conf, session);
            updateMigrationStatusIsCompleted(conf.getMigrationStatusKey());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] is completed");
        }
        appLogger.info("migration of independent attributes completed");

        // entities
        appLogger.info("migrating entities...");
        migrationBatch = getEntryMigrationConfigsByProcessorType(ProcessorType.ENTITY);
        for (EntryMigrationConfig conf : migrationBatch) {
            appLogger.info("starting migration for: [" + conf.getMigrationStatusKey() + "]");
            batchDataBuildQueriesAndInsert(conf, session);
            updateMigrationStatusIsCompleted(conf.getMigrationStatusKey());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] is completed");
        }
        appLogger.info("migration of entities completed");

        // relations containing only entities
        appLogger.info("migrating entity-relations...");
        migrationBatch = getEntryMigrationConfigsByProcessorType(ProcessorType.RELATION);
        for (EntryMigrationConfig conf : migrationBatch) {
            appLogger.info("starting migration for: [" + conf.getMigrationStatusKey() + "]");
            batchDataBuildQueriesAndInsert(conf, session);
            updateMigrationStatusIsCompleted(conf.getMigrationStatusKey());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] is completed");
        }
        appLogger.info("migration of entity-relations completed");

        // nested relations
        appLogger.info("migrating nested-relations...");
        migrationBatch = getEntryMigrationConfigsByProcessorType(ProcessorType.NESTED_RELATION);
        for (EntryMigrationConfig conf : migrationBatch) {
            appLogger.info("starting migration for: [" + conf.getMigrationStatusKey() + "]");
            batchDataBuildQueriesAndInsert(conf, session);
            updateMigrationStatusIsCompleted(conf.getMigrationStatusKey());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] is completed");
        }
        appLogger.info("migration of nested-relations completed");

        // append-or-write things
        appLogger.info("migrating append-or-insert things...");
        migrationBatch = getEntryMigrationConfigsByProcessorType(ProcessorType.APPEND_OR_INSERT);
        for (EntryMigrationConfig conf : migrationBatch) {
            appLogger.info("starting migration for: [" + conf.getMigrationStatusKey() + "]");
            batchDataBuildQueriesAndInsert(conf, session);
            updateMigrationStatusIsCompleted(conf.getMigrationStatusKey());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] is completed");
        }
        appLogger.info("migration of append-or-insert things completed");

        // append attributes
        appLogger.info("migrating append-attribute...");
        migrationBatch = getEntryMigrationConfigsByProcessorType(ProcessorType.APPEND_ATTRIBUTE);
        for (EntryMigrationConfig conf : migrationBatch) {
            appLogger.info("starting migration for: [" + conf.getMigrationStatusKey() + "]");
            batchDataBuildQueriesAndInsert(conf, session);
            updateMigrationStatusIsCompleted(conf.getMigrationStatusKey());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] is completed");
        }
        appLogger.info("migration of append-attribute completed");


        // attribute relations
        appLogger.info("migrating attribute-relations...");
        migrationBatch = getEntryMigrationConfigsByProcessorType(ProcessorType.ATTRIBUTE_RELATION);
        for (EntryMigrationConfig conf : migrationBatch) {
            appLogger.info("starting migration for: [" + conf.getMigrationStatusKey() + "]");
            batchDataBuildQueriesAndInsert(conf, session);
            updateMigrationStatusIsCompleted(conf.getMigrationStatusKey());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] is completed");
        }
        appLogger.info("migration of attribute-relations completed");


        // dependency migrations (go by number, low to high)
        appLogger.info("migrating order-after entries...");
        migrationBatch = getOrderedAfterEntryMigrationConfigs();
        for (EntryMigrationConfig conf : migrationBatch) {
            appLogger.info("starting migration for: [" + conf.getMigrationStatusKey() + "] of order-after " + conf.getDce().getOrderAfter());
            batchDataBuildQueriesAndInsert(conf, session);
            updateMigrationStatusIsCompleted(conf.getMigrationStatusKey());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] of order-after " + conf.getDce().getOrderAfter() + " is completed");
        }
        appLogger.info("migration of order-after entries completed");

    }

    private List<EntryMigrationConfig> getEntryMigrationConfigsByProcessorType(ProcessorType processorType) {
        List<EntryMigrationConfig> entries = new ArrayList<>();
        for (String dcEntryKey : dataConfig.keySet()) {
            ConfigEntryData dce = dataConfig.get(dcEntryKey);
            int dataPathIndex = 0;
            for (String dataPath : dce.getDataPath()) {
                ConfigEntryProcessor pce = getProcessorConfigEntry(dce.getProcessor());
                if (pce != null) {
                    String migrationStatusKey = dcEntryKey + "-" + dataPath;

                    if (isOfProcessorType(dce.getProcessor(), processorType) && dce.getOrderAfter() == null && dce.getOrderBefore() == null) {
                        if (migrationStatus != null && migrationStatus.get(migrationStatusKey) != null) { // previous migration present
                            appLogger.info("previous migration status found for: [" + migrationStatusKey + "]");
                            if (!migrationStatus.get(migrationStatusKey).isCompleted()) {
                                appLogger.info(migrationStatusKey + " not completely migrated yet, rows already migrated: " + migrationStatus.get(migrationStatusKey).getMigratedRows());
                                EntryMigrationConfig entry = new EntryMigrationConfig(dce, pce, dataPathIndex, migrationStatusKey, migrationStatus.get(migrationStatusKey).getMigratedRows(), getProcessor(dce, dataPathIndex), typeDBWriter);
                                entries.add(entry);
                            } else { // migration already completed
                                appLogger.info(migrationStatusKey + " is already completely migrated - moving on...");
                            }
                        } else { // no previous migration
                            appLogger.info("nothing previously migrated for [" + migrationStatusKey + "] - starting from row 0");
                            EntryMigrationConfig entry = new EntryMigrationConfig(dce, pce, dataPathIndex, migrationStatusKey, 0, getProcessor(dce, dataPathIndex), typeDBWriter);
                            entries.add(entry);
                        }
                    }
                    dataPathIndex = dataPathIndex + 1;
                } else {
                    appLogger.error("processor config entry for processor <" + dce.getProcessor() + "> not found");
                }
            }
        }
        return entries;
    }

    private List<EntryMigrationConfig> getOrderedAfterEntryMigrationConfigs() {
        List<EntryMigrationConfig> entries = new ArrayList<>();
        for (String dcEntryKey : dataConfig.keySet()) {
            ConfigEntryData dce = dataConfig.get(dcEntryKey);
            int dataPathIndex = 0;
            for (String dataPath : dce.getDataPath()) {
                ConfigEntryProcessor pce = getProcessorConfigEntry(dce.getProcessor());
                if (pce != null) {
                    String migrationStatusKey = dcEntryKey + "-" + dataPath;

                    if (dce.getOrderAfter() != null) {
                        if (migrationStatus != null && migrationStatus.get(migrationStatusKey) != null) { // previous migration present
                            appLogger.info("previous migration status found for ordered entry: [" + migrationStatusKey + "]");
                            if (!migrationStatus.get(migrationStatusKey).isCompleted()) {
                                appLogger.info(migrationStatusKey + " not completely migrated yet, rows already migrated: " + migrationStatus.get(migrationStatusKey).getMigratedRows());
                                EntryMigrationConfig entry = new EntryMigrationConfig(dce, pce, dataPathIndex, migrationStatusKey, migrationStatus.get(migrationStatusKey).getMigratedRows(), getProcessor(dce, dataPathIndex), typeDBWriter);
                                entries.add(entry);
                            } else { // migration already completed
                                appLogger.info(migrationStatusKey + " is already completely migrated - moving on...");
                            }
                        } else { // no previous migration
                            appLogger.info("nothing previously migrated for ordered entry [" + migrationStatusKey + "] - starting with row 0");
                            EntryMigrationConfig entry = new EntryMigrationConfig(dce, pce, dataPathIndex, migrationStatusKey, 0, getProcessor(dce, dataPathIndex), typeDBWriter);
                            entries.add(entry);
                        }
                    }
                    dataPathIndex = dataPathIndex + 1;
                } else {
                    appLogger.error("processor config entry for processor <" + dce.getProcessor() + "> not found");
                }
            }
        }
        entries.sort(Comparator.comparing(entry -> entry.getDce().getOrderAfter()));
        return entries;
    }

    private boolean isOfProcessorType(String dataConfigProcessorKey, ProcessorType processorType) {
        for (ConfigEntryProcessor gce : loaderLoadConfig.getProcessorConfig().get("processors")) {
            if (gce.getProcessor().equals(dataConfigProcessorKey)) {
                return gce.getProcessorType().equals(processorType);
            }
        }
        return false;
    }

    private ConfigEntryProcessor getProcessorConfigEntry(String dataConfigProcessorKey) {
        for (ConfigEntryProcessor gce : loaderLoadConfig.getProcessorConfig().get("processors")) {
            if (gce.getProcessor().equals(dataConfigProcessorKey)) {
                return gce;
            }
        }
        return null;
    }

    private void batchDataBuildQueriesAndInsert(EntryMigrationConfig conf, TypeDBSession session) {

        appLogger.info("inserting using " + conf.getDce().getThreads() + " threads" + " with thread commit size of " + conf.getDce().getBatchSize() + " rows");

        InputStream entityStream = FileToInputStream.getInputStream(conf.getDce().getDataPath()[conf.getDataPathIndex()]);
        String header = "";
        ArrayList<String> rows = new ArrayList<>();
        String line;
        int batchSizeCounter = 0;
        int totalRecordCounter = 0;
        double timerStart = System.currentTimeMillis();

        assert entityStream != null;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(entityStream))) {
            while ((line = br.readLine()) != null) {
                totalRecordCounter++;
                // get header of file
                if (totalRecordCounter == 1) {
                    header = line;
                    continue;
                }

                // skip over all lines already added into KG
                if (totalRecordCounter <= conf.getMigratedRows()) {
                    continue;
                }
                // write Batch once chunk size is reached
                rows.add(line);
                batchSizeCounter++;
                //                    if (batchSizeCounter == dce.getBatchSize()) {
                if (batchSizeCounter == conf.getDce().getBatchSize() * conf.getDce().getThreads()) {
                    System.out.print("+");
                    System.out.flush();
                    buildQueriesAndInsert(conf, session, rows, batchSizeCounter, totalRecordCounter, header);
                    batchSizeCounter = 0;
                    rows.clear();
                }
                // logging
                if (totalRecordCounter % 50000 == 0) {
                    System.out.flush();
                    appLogger.info("processed " + totalRecordCounter / 1000 + "k rows");
                }
            }
            //write the rest when loop exits with less than batch size
            if (!rows.isEmpty()) {
                buildQueriesAndInsert(conf, session, rows, batchSizeCounter, totalRecordCounter, header);
                if (totalRecordCounter % 50000 != 0) {
                    System.out.flush();
                }
            }

            appLogger.info("final # rows processed: " + totalRecordCounter);
            appLogger.info(logInsertRate(timerStart, totalRecordCounter));


        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void buildQueriesAndInsert(EntryMigrationConfig conf, TypeDBSession session, ArrayList<String> rows, int lineCounter, int rowCounter, String header) throws IOException {
        int threads = conf.getDce().getThreads();
        try {
            InsertQueries statements = conf.getInsertGenerator().typeDBInsert(rows, header, rowCounter - lineCounter);
            if (isOfProcessorType(conf.getDce().getProcessor(), ProcessorType.ENTITY) ||
                    isOfProcessorType(conf.getDce().getProcessor(), ProcessorType.ATTRIBUTE)) {
                appLogger.trace("number of generated insert Statements: " + statements.getDirectInserts().size());
                typeDBWriter.insertThreadedInserting(statements.getDirectInserts(), session, threads, conf.getDce().getBatchSize());
            } else if (isOfProcessorType(conf.getDce().getProcessor(), ProcessorType.RELATION) ||
                    isOfProcessorType(conf.getDce().getProcessor(), ProcessorType.NESTED_RELATION) ||
                    isOfProcessorType(conf.getDce().getProcessor(), ProcessorType.ATTRIBUTE_RELATION) ||
                    isOfProcessorType(conf.getDce().getProcessor(), ProcessorType.APPEND_ATTRIBUTE)) {
                appLogger.trace("number of generated insert Statements: " + statements.getMatchInserts().size());
                typeDBWriter.matchInsertThreadedInserting(statements, session, threads, conf.getDce().getBatchSize());
            } else if (isOfProcessorType(conf.getDce().getProcessor(), ProcessorType.APPEND_OR_INSERT)) {
                appLogger.trace("number of generated insert Statements: " + statements.getMatchInserts().size());
                typeDBWriter.appendOrInsertThreadedInserting(statements, session, threads, conf.getDce().getBatchSize());
            } else {
                throw new IllegalArgumentException("the processor <" + conf.getDce().getProcessor() + "> is not known - please check your processor config");
            }
            updateMigrationStatusMigratedRows(conf, lineCounter);
        } catch (Exception ee) {
            ee.printStackTrace();
        }
    }

    private void clearMigrationStatusFile() throws IOException {
        new FileWriter(migrationStatePath, false).close();
    }

    private void initializeMigrationStatus() {
        BufferedReader bufferedReader;
        try {
            bufferedReader = new BufferedReader(new FileReader(migrationStatePath));
            Type MigrationStatusType = new TypeToken<HashMap<String, MigrationStatus>>() {
            }.getType();
            migrationStatus = new Gson().fromJson(bufferedReader, MigrationStatusType);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void updateMigrationStatusMigratedRows(EntryMigrationConfig conf, int lineCounter) throws IOException {
        try {
            Gson gson = new Gson();
            Type MigrationStatusMapType = new TypeToken<HashMap<String, MigrationStatus>>() {
            }.getType();

            if (migrationStatus != null) {
                if (migrationStatus.get(conf.getMigrationStatusKey()) != null) { //updating an existing entry
                    int updatedMigratedRows = migrationStatus.get(conf.getMigrationStatusKey()).getMigratedRows() + lineCounter;
                    migrationStatus.get(conf.getMigrationStatusKey()).setMigratedRows(updatedMigratedRows);
                } else { // writing new entry
                    migrationStatus.put(conf.getMigrationStatusKey(), new MigrationStatus(conf.getPce().getSchemaType(), false, lineCounter));
                }
            } else { //writing very first entry (i.e. file was empty)
                migrationStatus = new HashMap<>();
                migrationStatus.put(conf.getMigrationStatusKey(), new MigrationStatus(conf.getPce().getSchemaType(), false, lineCounter));
            }

            // update file
            FileWriter fw = new FileWriter(migrationStatePath);
            gson.toJson(migrationStatus, MigrationStatusMapType, fw);
            fw.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void updateMigrationStatusIsCompleted(String migrationStatusKey) throws IOException {
        try {
            Gson gson = new Gson();
            Type MigrationStatusMapType = new TypeToken<HashMap<String, MigrationStatus>>() {
            }.getType();
            migrationStatus.get(migrationStatusKey).setCompleted(true);
            FileWriter fw = new FileWriter(migrationStatePath);
            gson.toJson(migrationStatus, MigrationStatusMapType, fw);
            fw.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private InsertProcessor getProcessor(ConfigEntryData dce, int dataPathIndex) {
        ConfigEntryProcessor gce = getGenFromGenConfig(dce.getProcessor(), loaderLoadConfig.getProcessorConfig());

        if (gce != null) {
            switch (gce.getProcessorType()) {
                case ENTITY:
                    appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
                    return new EntityInsertProcessor(dce, gce, dataPathIndex);
                case RELATION:
                case ATTRIBUTE_RELATION:
                    appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
                    return new RelationInsertProcessor(dce, gce, dataPathIndex);
                case NESTED_RELATION:
                    appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
                    return new NestedRelationInsertProcessor(dce, gce, dataPathIndex);
                case APPEND_ATTRIBUTE:
                    appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
                    return new AppendAttributeProcessor(dce, gce, dataPathIndex);
                case ATTRIBUTE:
                    appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
                    return new AttributeInsertProcessor(dce, gce, dataPathIndex);
                case APPEND_OR_INSERT:
                    appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
                    return new AppendOrInsertProcessor(dce, gce, dataPathIndex);
                default:
                    throw new IllegalArgumentException(String.format("Invalid/No generator provided for: %s", dce.getProcessor()));
            }
        } else {
            throw new NullPointerException("ProcessorConfigEntry for " + dce.getProcessor() + " is null/cannot be found");
        }
    }

    private ConfigEntryProcessor getGenFromGenConfig(String processor, HashMap<String, ArrayList<ConfigEntryProcessor>> processorConfig) {
        for (ConfigEntryProcessor e : processorConfig.get("processors")) {
            if (e.getProcessor().equals(processor)) {
                return e;
            }
        }
        return null;
    }

    private String logInsertRate(double timerStart, int totalRecordCounter) {
        double timerEnd = System.currentTimeMillis();
        double secs = (timerEnd - timerStart) / 1000;
        DecimalFormat df = new DecimalFormat("#");
        df.setRoundingMode(RoundingMode.DOWN);
        if (secs > 0) {
            return "insert rate inserts/second: " + df.format((totalRecordCounter / secs));
        } else {
            return "insert rate inserts/second: superfast";
        }

    }

}