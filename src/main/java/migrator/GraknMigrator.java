package migrator;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import configuration.DataConfigEntry;
import configuration.MigrationConfig;
import configuration.MigrationStatus;
import configuration.ProcessorConfigEntry;
import generator.*;
import grakn.client.api.GraknClient;
import grakn.client.api.GraknSession;
import graql.lang.pattern.variable.ThingVariable;
import insert.GraknInserter;
import loader.DataLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.lang.reflect.Type;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;


public class GraknMigrator {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private final HashMap<String, DataConfigEntry> dataConfig;
    private final String migrationStatePath;
    private final GraknInserter graknInserter;
    private final MigrationConfig migrationConfig;
    private boolean cleanAndMigrate = false;
    private HashMap<String, MigrationStatus> migrationStatus;

    public GraknMigrator(MigrationConfig migrationConfig,
                         String migrationStatePath) {
        this.dataConfig = migrationConfig.getDataConfig();
        this.migrationStatePath = migrationStatePath;
        this.migrationConfig = migrationConfig;
        this.graknInserter = new GraknInserter(migrationConfig.getGraknURI().split(":")[0],
                migrationConfig.getGraknURI().split(":")[1],
                migrationConfig.getSchemaPath(),
                migrationConfig.getKeyspace()
        );
    }

    public GraknMigrator(MigrationConfig migrationConfig,
                         String migrationStatePath,
                         boolean cleanAndMigrate) throws IOException {
        this(migrationConfig, migrationStatePath);
        if (cleanAndMigrate) {
            this.cleanAndMigrate = true;
            clearMigrationStatusFile();
        }
    }

    public void migrate() throws IOException {

        validateConfigs();

        initializeMigrationStatus();
        GraknClient client = graknInserter.getClient();

        if (cleanAndMigrate) {
            graknInserter.cleanAndDefineSchemaToDatabase(client);
            appLogger.info("cleaned database and migrate schema...");
        } else {
            appLogger.info("using existing DB and schema to continue previous migration...");
        }

        GraknSession dataSession = graknInserter.getDataSession(client);
        migrateThingsInOrder(dataSession);

        dataSession.close();
        client.close();
        appLogger.info("GraMi is finished migrating your stuff!");
    }

    private void migrateThingsInOrder(GraknSession session) throws IOException {
        List<EntryMigrationConfig> migrationBatch;

        // independent attributes
        appLogger.info("migrating independent attributes...");
        migrationBatch = getEntryMigrationConfigsByProcessorType("attribute");
        for (EntryMigrationConfig conf : migrationBatch) {
            appLogger.info("starting migration for: [" + conf.getMigrationStatusKey() + "]");
            batchDataBuildQueriesAndInsert(conf, session);
            updateMigrationStatusIsCompleted(conf.getMigrationStatusKey());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] is completed");
        }
        appLogger.info("migration of independent attributes completed");

        // entities
        appLogger.info("migrating entities...");
        migrationBatch = getEntryMigrationConfigsByProcessorType("entity");
        for (EntryMigrationConfig conf : migrationBatch) {
            appLogger.info("starting migration for: [" + conf.getMigrationStatusKey() + "]");
            batchDataBuildQueriesAndInsert(conf, session);
            updateMigrationStatusIsCompleted(conf.getMigrationStatusKey());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] is completed");
        }
        appLogger.info("migration of entities completed");

        // relations containing only entities
        appLogger.info("migrating entity-relations...");
        migrationBatch = getEntryMigrationConfigsByProcessorType("relation");
        for (EntryMigrationConfig conf : migrationBatch) {
            appLogger.info("starting migration for: [" + conf.getMigrationStatusKey() + "]");
            batchDataBuildQueriesAndInsert(conf, session);
            updateMigrationStatusIsCompleted(conf.getMigrationStatusKey());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] is completed");
        }
        appLogger.info("migration of entity-relations completed");

        // nested relations
        appLogger.info("migrating nested-relations...");
        migrationBatch = getEntryMigrationConfigsByProcessorType("nested-relation");
        for (EntryMigrationConfig conf : migrationBatch) {
            appLogger.info("starting migration for: [" + conf.getMigrationStatusKey() + "]");
            batchDataBuildQueriesAndInsert(conf, session);
            updateMigrationStatusIsCompleted(conf.getMigrationStatusKey());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] is completed");
        }
        appLogger.info("migration of nested-relations completed");

        // append attributes
        appLogger.info("migrating append-attribute...");
        migrationBatch = getEntryMigrationConfigsByProcessorType("append-attribute");
        for (EntryMigrationConfig conf : migrationBatch) {
            appLogger.info("starting migration for: [" + conf.getMigrationStatusKey() + "]");
            batchDataBuildQueriesAndInsert(conf, session);
            updateMigrationStatusIsCompleted(conf.getMigrationStatusKey());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] is completed");
        }
        appLogger.info("migration of append-attribute completed");


        // attribute relations
        appLogger.info("migrating attribute-relations...");
        migrationBatch = getEntryMigrationConfigsByProcessorType("attribute-relation");
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
            System.out.println(conf.getDce().getOrderAfter());
            appLogger.info("migration for: [" + conf.getMigrationStatusKey() + "] of order-after " + conf.getDce().getOrderAfter() + " is completed");
        }
        appLogger.info("migration of order-after entries completed");

    }

    private List<EntryMigrationConfig> getEntryMigrationConfigsByProcessorType(String processorType) {
        List<EntryMigrationConfig> entries = new ArrayList<>();
        for (String dcEntryKey : dataConfig.keySet()) {
            DataConfigEntry dce = dataConfig.get(dcEntryKey);
            int dataPathIndex = 0;
            for (String dataPath : dce.getDataPath()){
                ProcessorConfigEntry pce = getProcessorConfigEntry(dce.getProcessor());
                String migrationStatusKey = dcEntryKey + "-" + dataPath;

                if (isOfProcessorType(dce.getProcessor(), processorType) && dce.getOrderAfter() == null && dce.getOrderBefore() == null) {
                    if (migrationStatus != null && migrationStatus.get(migrationStatusKey) != null) { // previous migration present
                        appLogger.info("previous migration status found for: [" + migrationStatusKey + "]");
                        if (!migrationStatus.get(migrationStatusKey).isCompleted()) {
                            appLogger.info(migrationStatusKey + " not completely migrated yet, rows already migrated: " + migrationStatus.get(migrationStatusKey).getMigratedRows());
                            EntryMigrationConfig entry = new EntryMigrationConfig(dce, pce, dataPathIndex, migrationStatusKey, migrationStatus.get(migrationStatusKey).getMigratedRows(), getProcessor(dce, dataPathIndex));
                            entries.add(entry);
                        } else { // migration already completed
                            appLogger.info(migrationStatusKey + " is already completely migrated - moving on...");
                        }
                    } else { // no previous migration
                        appLogger.info("nothing previously migrated for [" + migrationStatusKey + "] - starting from row 0");
                        EntryMigrationConfig entry = new EntryMigrationConfig(dce, pce, dataPathIndex, migrationStatusKey, 0, getProcessor(dce, dataPathIndex));
                        entries.add(entry);
                    }
                }
                dataPathIndex = dataPathIndex + 1;
            }
        }
        return entries;
    }

    private List<EntryMigrationConfig> getOrderedAfterEntryMigrationConfigs() {
        List<EntryMigrationConfig> entries = new ArrayList<>();
        for (String dcEntryKey : dataConfig.keySet()) {
            DataConfigEntry dce = dataConfig.get(dcEntryKey);
            int dataPathIndex = 0;
            for (String dataPath : dce.getDataPath()) {
                ProcessorConfigEntry pce = getProcessorConfigEntry(dce.getProcessor());
                String migrationStatusKey = dcEntryKey + "-" + dataPath;

                if (dce.getOrderAfter() != null) {
                    if (migrationStatus != null && migrationStatus.get(migrationStatusKey) != null) { // previous migration present
                        appLogger.info("previous migration status found for ordered entry: [" + migrationStatusKey + "]");
                        if (!migrationStatus.get(migrationStatusKey).isCompleted()) {
                            appLogger.info(migrationStatusKey + " not completely migrated yet, rows already migrated: " + migrationStatus.get(migrationStatusKey).getMigratedRows());
                            EntryMigrationConfig entry = new EntryMigrationConfig(dce, pce, dataPathIndex, migrationStatusKey, migrationStatus.get(migrationStatusKey).getMigratedRows(), getProcessor(dce, dataPathIndex));
                            entries.add(entry);
                        } else { // migration already completed
                            appLogger.info(migrationStatusKey + " is already completely migrated - moving on...");
                        }
                    } else { // no previous migration
                        appLogger.info("nothing previously migrated for ordered entry [" + migrationStatusKey + "] - starting with row 0");
                        EntryMigrationConfig entry = new EntryMigrationConfig(dce, pce, dataPathIndex, migrationStatusKey, 0, getProcessor(dce, dataPathIndex));
                        entries.add(entry);
                    }
                }
                dataPathIndex = dataPathIndex + 1;
            }
        }
        entries.sort(Comparator.comparing(entry -> entry.getDce().getOrderAfter()));
        return entries;
    }

    private boolean isOfProcessorType(String key, String conceptType) {
        for (ProcessorConfigEntry gce : migrationConfig.getProcessorConfig().get("processors")) {
            if (gce.getProcessor().equals(key)) {
                return gce.getProcessorType().equals(conceptType);
            }
        }
        return false;
    }

    private ProcessorConfigEntry getProcessorConfigEntry(String key) {
        for (ProcessorConfigEntry gce : migrationConfig.getProcessorConfig().get("processors")) {
            if (gce.getProcessor().equals(key)) {
                return gce;
            }
        }
        return null;
    }

    private void batchDataBuildQueriesAndInsert(EntryMigrationConfig conf, GraknSession session) {

        appLogger.info("inserting using " + conf.getDce().getThreads() + " threads" + " with thread commit size of " + conf.getDce().getBatchSize() + " rows");

        InputStream entityStream = DataLoader.getInputStream(conf.getDce().getDataPath()[conf.getDataPathIndex()]);
        String header = "";
        ArrayList<String> rows = new ArrayList<>();
        String line;
        int batchSizeCounter = 0;
        int totalRecordCounter = 0;
        double timerStart = System.currentTimeMillis();

        if (entityStream != null) {
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
                    // insert Batch once chunk size is reached
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
                        System.out.println();
                        appLogger.info("processed " + totalRecordCounter / 1000 + "k rows");
                    }
                }
                //insert the rest when loop exits with less than batch size
                if (!rows.isEmpty()) {
                    buildQueriesAndInsert(conf, session, rows, batchSizeCounter, totalRecordCounter, header);
                    if (totalRecordCounter % 50000 != 0) {
                        System.out.println();
                    }
                }

                appLogger.info("final # rows processed: " + totalRecordCounter);
                appLogger.info(logInsertRate(timerStart, totalRecordCounter));


            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private void buildQueriesAndInsert(EntryMigrationConfig conf, GraknSession session, ArrayList<String> rows, int lineCounter, int rowCounter, String header) throws IOException {
        int threads = conf.getDce().getThreads();
        try {
            if (isOfProcessorType(conf.getDce().getProcessor(), "entity")) {
                ArrayList<ThingVariable<?>> insertStatements = conf.getInsertGenerator().graknEntityInsert(rows, header, rowCounter - lineCounter);
                appLogger.trace("number of generated insert Statements: " + insertStatements.size());
                graknInserter.insertThreadedInserting(insertStatements, session, threads, conf.getDce().getBatchSize());

            } else if (isOfProcessorType(conf.getDce().getProcessor(), "relation") ||
                    isOfProcessorType(conf.getDce().getProcessor(), "nested-relation") ||
                    isOfProcessorType(conf.getDce().getProcessor(), "attribute-relation")) {
                HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> statements = conf.getInsertGenerator().graknRelationInsert(rows, header, rowCounter - lineCounter);
                appLogger.trace("number of generated insert Statements: " + statements.get("match").size());
                graknInserter.matchInsertThreadedInserting(statements, session, threads, conf.getDce().getBatchSize());

            } else if (isOfProcessorType(conf.getDce().getProcessor(), "append-attribute")) {
                HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> statements = conf.getInsertGenerator().graknAppendAttributeInsert(rows, header, rowCounter - lineCounter);
                appLogger.trace("number of generated insert Statements: " + statements.get("match").size());
                graknInserter.matchInsertThreadedInserting(statements, session, threads, conf.getDce().getBatchSize());

            } else if (isOfProcessorType(conf.getDce().getProcessor(), "attribute")) {
                ArrayList<ThingVariable<?>> statements = conf.getInsertGenerator().graknAttributeInsert(rows, header, rowCounter - lineCounter);
                appLogger.trace("number of generated insert Statements: " + statements.size());
                graknInserter.insertThreadedInserting(statements, session, threads, conf.getDce().getBatchSize());
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

    private InsertGenerator getProcessor(DataConfigEntry dce, int dataPathIndex) {
        ProcessorConfigEntry gce = getGenFromGenConfig(dce.getProcessor(), migrationConfig.getProcessorConfig());

        if (gce != null && gce.getProcessorType().equals("entity")) {
            appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
            return new EntityInsertGenerator(dce, gce, dataPathIndex);
        } else if (gce != null && gce.getProcessorType().equals("relation")) {
            appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
            return new RelationInsertGenerator(dce, gce, dataPathIndex);
        } else if (gce != null && gce.getProcessorType().equals("nested-relation")) {
            appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
            return new NestedRelationInsertGenerator(dce, gce, dataPathIndex);
        } else if (gce != null && gce.getProcessorType().equals("attribute-relation")) {
            appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
            return new RelationInsertGenerator(dce, gce, dataPathIndex);
        } else if (gce != null && gce.getProcessorType().equals("append-attribute")) {
            appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
            return new AppendAttributeGenerator(dce, gce, dataPathIndex);
        } else if (gce != null && gce.getProcessorType().equals("attribute")) {
            appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
            return new AttributeInsertGenerator(dce, gce, dataPathIndex);
        } else {
            throw new IllegalArgumentException(String.format("Invalid/No generator provided for: %s", dce.getProcessor()));
        }
    }

    private ProcessorConfigEntry getGenFromGenConfig(String processor, HashMap<String, ArrayList<ProcessorConfigEntry>> processorConfig) {
        for (ProcessorConfigEntry e : processorConfig.get("processors")) {
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

    public ArrayList<ValidationReport> validateConfigs() {
        ArrayList<ValidationReport> reports = new ArrayList<>();
        reports.add(validateProcessorConfigAgainstSchema());
        reports.add(validateDataConfigAgainstProcessorConfig());
        return reports;
    }

    private ValidationReport validateProcessorConfigAgainstSchema() {
        HashMap<String, ArrayList<ProcessorConfigEntry>> procConfig = migrationConfig.getProcessorConfig();

        ArrayList<String> errors = new ArrayList<>();

        for (Map.Entry<String, ArrayList<ProcessorConfigEntry>> entry: procConfig.entrySet()) {
            ArrayList<ProcessorConfigEntry> processors = entry.getValue();
            int processorIndex = 0;
            for (ProcessorConfigEntry processor : processors) {
                String processorName = processor.getProcessor();
                if(processorName.isEmpty()) {
                    errors.add("ProcessorConfigEntry at index " + processorIndex + " has an empty \"processor\" field");
                }
                String res = validateProcessorType(processor.getProcessorType());
                if (!res.isEmpty()) errors.add("In Processor <" + processorName + ">: " + res);

                processorIndex = processorIndex + 1;
            }
        }

        // for each entry:
            // "processorType" must be one of enum of entity, relation, nested-relation, etc...
            // "schemaType" must exist
            // "processor" must be unique and non-empty
            // if entity
                // conceptGenerators and attributes must be present
                // foreach attribute
                    // key must be unique and non-empty
                    // "attributeType" must specify existing attribute of "schemaType" & have correct "valueType"
                    // "required" must be present as either false/true (boolean)
            // if relation
                // conceptGenerators and players must be present, attributes optionally
                // for each attribute
                    // key must be unique and non-empty
                    // "attributeType" must specify existing attribute of "schemaType" & have correct "valueType"
                    // "required" must be present as either false/true (boolean)
                // for each player
                    // key must be unique and non-empty
                    // "playerType" must refer to existing "schemaType" and play "roleType" and be an entity
                    // "roleType" must be a role in "schemaType"
                    // "uniquePlayerId" must be existing attribute for "playerType" & "idValueType" must be of correct value type
                    // "required" must be present as either false/true (boolean)
            // if nested-relation
                // conceptGenerators and relationPlayers must be present, attributes and players optionally present
                    // for each attribute
                        // key must be unique and non-empty
                        // "attributeType" must specify existing attribute of "schemaType" & have correct "valueType"
                        // "required" must be present as either false/true (boolean)
                    // for each player
                        // key must be unique and non-empty
                        // "playerType" must refer to existing "schemaType" and play "roleType" and be an entity
                        // "roleType" must be a role in "schemaType"
                        // "uniquePlayerId" must be existing attribute for "playerType" & "idValueType" must be of correct value type
                        // "required" must be present as either false/true (boolean)
                    // for each "relationPlayers"
                        // key must be unique and non-empty
                        // "playerType" must exist as relation in schema and must play "roleType" in "SchemaType" relation
                        // required must be present as either false/true
                        // must have either "matchByAttribute" or "matchByPlayer", or both
                        // foreach "matchByAttribute"
                            // key must be unique and non-empty
                            // "attributeType" must exist as attribute for "playerType" and "valueType" must be correct
                        // foreach matchByPlayer
                            // key must be unique and non-empty
                            // "playerType" must exist and be an entity
                            // "uniquePlayerId" must be attribute of "playerType" and have correct "idValueType"
                            // "playerType" must play "roleType" and "roleType" must be role of parent-"playerType" relation
                            // required must be present as either false/true
            // if append-attribute
                // conceptGenerators and attributes must be present
                // foreach attribute
                    // key must be present as either true/false (boolean)
                    // "attributeType" must be an attribute for "schemaType" and be of correct "valueType"
                    // required must be present as either true/false --> only for those that are not special case in dataconfig!!!
            // if attribute
                // conceptGenerators and attributes must be present
                // foreach attribute
                    // only a single key allowed must be unique and non-empty
                    // "attributeType" must be an attribute in schema with correct "valueType"
                    // all other fields are disallowed
            // if attribute relation
                // conceptGenerators and players must be present, attributes optional
                // foreach attribute as above
                // foreach player
                    // key must be unique and non-empty
                    // "playerType" must be an attribute or an entity
                    // if attribute:
                        // "uniquePlayerId" must be "_attribute_player_"
                        // "idValueType must be the correct value type
                        // "roleType" must be played by the attribute and the relation must have it as a player
                        // required must be present as boolean true/false
                    // if entity
                        // do above validation
        return new ValidationReport("proc-config", errors.size() <= 0, errors);
    }

    private String validateProcessorType(String processorType) {
        boolean flag = false;
        for (ProcessorTypes proctype: ProcessorTypes.values()) {
            if (proctype.toString().equals(processorType)) {
                flag = true;
                break;
            }
        }
        if (flag) {
            return "";
        }
        return processorType + " is not a valid processorType";
    }

    private ValidationReport validateDataConfigAgainstProcessorConfig() {
        ArrayList<String> errors = new ArrayList<>();
        return new ValidationReport("data-config", errors.size() <= 0, errors);
    }

    private boolean keysNotEmpty(ArrayList<String> keys) {
        for (String key : keys) {
            if (key.isEmpty()) {
                return false;
            }
        }
        return true;
    }

}
