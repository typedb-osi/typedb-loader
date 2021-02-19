package migrator;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import configuration.*;
import generator.*;
import grakn.client.GraknClient;
import grakn.client.GraknClient.Session;
import graql.lang.pattern.variable.ThingVariable;
import loader.DataLoader;
import insert.GraknInserter;
import java.io.*;
import java.lang.reflect.Type;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GraknMigrator {

    private final HashMap<String, DataConfigEntry> dataConfig;
    private final String migrationStatePath;
    private boolean cleanAndMigrate = false;
    private HashMap<String, MigrationStatus> migrationStatus;
    private final GraknInserter graknInserter;
    private final MigrationConfig migrationConfig;
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");

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

    public void migrate(boolean migrateEntities, boolean migrateRelations, boolean migrateRelationRelations, boolean migrateAppendAttributes) throws IOException {

        initializeMigrationStatus();
        GraknClient client = graknInserter.getClient();

        if (cleanAndMigrate) {
            graknInserter.cleanAndDefineSchemaToDatabase(client);
            appLogger.info("cleaned database and migrate schema...");
        } else {
            appLogger.info("using existing DB and schema to continue previous migration...");
        }

        GraknClient.Session dataSession = graknInserter.getDataSession(client);
        migrateThingsInOrder(dataSession, migrateEntities, migrateRelations, migrateRelationRelations, migrateAppendAttributes);

        dataSession.close();
        client.close();
        appLogger.info("GraMi is finished migrating your stuff!");
    }

    private void migrateThingsInOrder(Session session, boolean migrateEntities, boolean migrateRelations, boolean migrateRelationRelations, boolean migrateAppendAttributes) throws IOException {
        if (migrateEntities) {
            appLogger.info("migrating entities...");
            getStatusAndMigrate(session, "entity");
            appLogger.info("migration of entities completed");
        }
        if (migrateEntities && migrateRelations) {
            appLogger.info("migrating relations...");
            getStatusAndMigrate(session, "relation");
            appLogger.info("migration of relations completed");
        }
        if (migrateEntities && migrateRelations && migrateRelationRelations) {
            appLogger.info("migrating relation-with-relations...");
            getStatusAndMigrate(session, "relation-with-relation");
            appLogger.info("migration of relation-with-relations completed");
        }
        if (migrateEntities && migrateRelations && migrateRelationRelations && migrateAppendAttributes) {
            appLogger.info("migrating append-attribute...");
            getStatusAndMigrate(session, "append-attribute");
            appLogger.info("migration of append-attribute completed");
        }
    }

    private void getStatusAndMigrate(Session session, String processorType) throws IOException {
        for (String dcEntryKey : dataConfig.keySet()) {
            DataConfigEntry dce = dataConfig.get(dcEntryKey);
            String currentProcessor = dce.getProcessor();
            if(isOfProcessorType(currentProcessor, processorType)){
                appLogger.info("migrating [" + dcEntryKey + "]...");
                if (migrationStatus != null && migrationStatus.get(dce.getDataPath()) != null) {
                    appLogger.info("previous migration status found for schema type: [" + dcEntryKey + "]");
                    if (!migrationStatus.get(dce.getDataPath()).isCompleted()) {
                        appLogger.info(dcEntryKey + " not completely migrated yet, rows already migrated: " + migrationStatus.get(dce.getDataPath()).getMigratedRows());
                        getGeneratorAndInsert(session, dce, migrationStatus.get(dce.getDataPath()).getMigratedRows());
                    } else {
                        appLogger.info(dcEntryKey + " is already completely migrated - moving on...");
                    }
                } else {
                    appLogger.info("nothing previously migrated for [" + dcEntryKey + "] - starting with row 0");
                    getGeneratorAndInsert(session, dce, 0);
                }
            }
        }
    }

    private boolean isOfProcessorType(String key, String conceptType) {
        for (ProcessorConfigEntry gce : migrationConfig.getProcessorConfig().get("processors")) {
            if (gce.getProcessor().equals(key)) {
                return gce.getProcessorType().equals(conceptType);
            }
        }
        return false;
    }

    private void getGeneratorAndInsert(Session session, DataConfigEntry dce, int skipRows) throws IOException {
        // choose insert generator
        InsertGenerator gen = getProcessor(dce);

        writeThingToGrakn(dce, gen, session, skipRows);
        updateMigrationStatusIsCompleted(dce);
    }

    private void writeThingToGrakn(DataConfigEntry dce, InsertGenerator gen, Session session, int skipLines) {

        appLogger.info("inserting using " + dce.getThreads() + " threads" + " with thread commit size of " + dce.getBatchSize() + " rows");

        InputStream entityStream = DataLoader.getInputStream(dce.getDataPath());
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
                    if (totalRecordCounter <= skipLines) {
                        continue;
                    }
                    // insert Batch once chunk size is reached
                    rows.add(line);
                    batchSizeCounter++;
//                    if (batchSizeCounter == dce.getBatchSize()) {
                    if (batchSizeCounter == dce.getBatchSize() * dce.getThreads()) {
                        System.out.print("+");
                        System.out.flush();
                        writeThing(dce, gen, session, rows, batchSizeCounter, header);
                        batchSizeCounter = 0;
                        rows.clear();
                    }
                    // logging
                    if (totalRecordCounter % 50000 == 0) {
                        System.out.println();
                        appLogger.info("processed " + totalRecordCounter/1000 + "k rows");
                    }
                }
                //insert the rest when loop exits with less than batch size
                if (!rows.isEmpty()) {
                    writeThing(dce, gen, session, rows, batchSizeCounter, header);
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



    private void writeThing(DataConfigEntry dce, InsertGenerator gen, Session session, ArrayList<String> rows, int lineCounter, String header) throws IOException {
        int threads = dce.getThreads();
        try {
            if (isOfProcessorType(dce.getProcessor(), "entity")) {
                ArrayList<ThingVariable<?>> insertStatements = gen.graknEntityInsert(rows, header);
                appLogger.trace("number of generated insert Statements: " + insertStatements.size());
                graknInserter.insertThreadedInserting(insertStatements, session, threads, dce.getBatchSize());
            } else if (isOfProcessorType(dce.getProcessor(), "relation") ||
                    isOfProcessorType(dce.getProcessor(), "relation-with-relation")) {
                HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> statements = gen.graknRelationInsert(rows, header);
                appLogger.trace("number of generated insert Statements: " + statements.get("match").size());
                graknInserter.matchInsertThreadedInserting(statements, session, threads, dce.getBatchSize());
            } else if (isOfProcessorType(dce.getProcessor(), "append-attribute")) {
                HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> statements = gen.graknAppendAttributeInsert(rows, header);
                appLogger.trace("number of generated insert Statements: " + statements.get("match").size());
                graknInserter.matchInsertThreadedInserting(statements, session, threads, dce.getBatchSize());
            } else {
                throw new IllegalArgumentException("the processor <" + dce.getProcessor() + "> is not known");
            }
            updateMigrationStatusMigratedRows(dce, lineCounter);
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
            Type MigrationStatusType = new TypeToken<HashMap<String, MigrationStatus>>() {}.getType();
            migrationStatus = new Gson().fromJson(bufferedReader, MigrationStatusType);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void updateMigrationStatusMigratedRows(DataConfigEntry dce, int lineCounter) throws IOException {
        try {
            ProcessorConfigEntry pce = getGenFromGenConfig(dce.getProcessor(), migrationConfig.getProcessorConfig());
            Gson gson = new Gson();
            Type MigrationStatusMapType = new TypeToken<HashMap<String, MigrationStatus>>(){}.getType();

            if (migrationStatus != null) {
                if (migrationStatus.get(dce.getDataPath()) != null) { //updating an existing entry
                    int updatedMigratedRows = migrationStatus.get(dce.getDataPath()).getMigratedRows() + lineCounter;
                    migrationStatus.get(dce.getDataPath()).setMigratedRows(updatedMigratedRows);
                } else { // writing new entry
                    migrationStatus.put(dce.getDataPath(), new MigrationStatus(pce.getSchemaType(), false, lineCounter));
                }
            } else { //writing very first entry (i.e. file was empty)
                migrationStatus = new HashMap<>();
                migrationStatus.put(dce.getDataPath(), new MigrationStatus(pce.getSchemaType(), false, lineCounter));
            }

            // update file
            FileWriter fw = new FileWriter(migrationStatePath);
            gson.toJson(migrationStatus, MigrationStatusMapType, fw);
            fw.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void updateMigrationStatusIsCompleted(DataConfigEntry dce) throws IOException {
        try {
            Gson gson = new Gson();
            Type MigrationStatusMapType = new TypeToken<HashMap<String, MigrationStatus>>(){}.getType();
            migrationStatus.get(dce.getDataPath()).setCompleted(true);
            FileWriter fw = new FileWriter(migrationStatePath);
            gson.toJson(migrationStatus, MigrationStatusMapType, fw);
            fw.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private InsertGenerator getProcessor(DataConfigEntry dce) {
        ProcessorConfigEntry gce = getGenFromGenConfig(dce.getProcessor(), migrationConfig.getProcessorConfig());

        if (gce != null && gce.getProcessorType().equals("entity")) {
            appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
            return new EntityInsertGenerator(dce, gce);
        } else if (gce != null && gce.getProcessorType().equals("relation")) {
            appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
            return new RelationInsertGenerator(dce, gce);
        } else if (gce != null && gce.getProcessorType().equals("relation-with-relation")) {
            appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
            return new RelationWithRelationInsertGenerator(dce, gce);
        } else if (gce != null && gce.getProcessorType().equals("append-attribute")) {
            appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
            return new AppendAttributeGenerator(dce, gce);
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

}
