package migrator;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import configuration.*;
import loader.DataLoader;
import grakn.client.GraknClient;
import insert.GraknInserter;
import graql.lang.statement.Statement;
import generator.EntityInsertGenerator;
import generator.InsertGenerator;

import java.io.*;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import generator.RelationInsertGenerator;


public class GraknMigrator {

    private final HashMap<String, DataConfigEntry> dataConfig;
    private final String migrationStatePath;
    private boolean cleanAndMigrate = false;
    private HashMap<String, MigrationStatus> migrationStatus;
    private final GraknInserter gm;
    private final MigrationConfig migrationConfig;
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");

    public GraknMigrator(MigrationConfig migrationConfig,
                         String migrationStatePath) {
        this.dataConfig = migrationConfig.getDataConfig();
        this.migrationStatePath = migrationStatePath;
        this.migrationConfig = migrationConfig;
        this.gm = new GraknInserter(migrationConfig.getGraknURI().split(":")[0],
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

    public void migrate(boolean migrateEntities, boolean migrateRelations) throws IOException {

        GraknClient client = gm.getClient();
        GraknClient.Session session = gm.getSession(client);

        getMigrationStatus();

        if (cleanAndMigrate) {
            session = gm.setKeyspaceToSchema(client, session);
            appLogger.info("cleaned and reloaded keyspace");
        } else {
            appLogger.info("continuing previous migration...");
        }

        migrateThingsInOrder(session, migrateEntities, migrateRelations);

        session.close();
        client.close();
    }

    private void migrateThingsInOrder(GraknClient.Session session, boolean migrateEntities, boolean migrateRelations) throws IOException {
        if (migrateEntities) {
            appLogger.info("migrating entities...");
            getStatusAndMigrate(session, "entity");
            appLogger.info("migration of entities completed");
        }
        if (migrateRelations && migrateEntities) {
            appLogger.info("migrating relations...");
            getStatusAndMigrate(session, "relation");
            appLogger.info("migration of relations completed");
        }
    }

    private void getStatusAndMigrate(GraknClient.Session session, String conceptType) throws IOException {
        for (String dcEntryKey : dataConfig.keySet()) {
            DataConfigEntry dce = dataConfig.get(dcEntryKey);
            String currentProcessor = dce.getProcessor();
            if(isOfConceptType(currentProcessor, conceptType)){
                appLogger.info("migrating [" + dcEntryKey + "]...");
                if (migrationStatus != null && migrationStatus.get(dcEntryKey) != null) {
                    appLogger.info("previous migration status found for entity type: [" + dcEntryKey + "]");
                    if (!migrationStatus.get(dcEntryKey).isCompleted()) {
                        appLogger.info(dcEntryKey + " not completely migrated yet, rows already migrated: " + migrationStatus.get(dcEntryKey).getMigratedRows());
                        getGeneratorAndInsert(session, dce, migrationStatus.get(dcEntryKey).getMigratedRows());
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

    private boolean isOfConceptType(String key, String conceptType) {
        for (ProcessorConfigEntry gce : migrationConfig.getProcessorConfig().get("processors")) {
            if (gce.getProcessor().equals(key)) {
                if (gce.getProcessorType().equals(conceptType)) {
                    return true;
                } else {
                    return false;
                }
            }
        }
        return false;
    }

    private void getGeneratorAndInsert(GraknClient.Session session, DataConfigEntry dce, int skipRows) throws IOException {
        // choose insert generator
        InsertGenerator gen = getProcessor(dce);

        writeThingToGrakn(dce, gen, session, skipRows);
        updateMigrationStatusIsCompleted(dce);
    }

    private void writeThingToGrakn(DataConfigEntry dce, InsertGenerator gen, GraknClient.Session session, int skipLines) {
        InputStream entityStream = DataLoader.getInputStream(dce.getDataPath());
        String header = "";
        ArrayList<String> rows = new ArrayList<>();
        String line;
        int batchSizeCounter = 0;
        int totalRecordCounter = 0;

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
                    if (batchSizeCounter == dce.getBatchSize()) {
                        writeThing(dce, gen, session, rows, batchSizeCounter, header);
                        batchSizeCounter = 0;
                        rows.clear();
                    }
                    // logging
                    if (totalRecordCounter % 50000 == 0) {
                        appLogger.info("progress: # rows processed so far (k): " + totalRecordCounter/1000);
                    }
                }
                //insert the rest when loop exits with less than batch size
                if (!rows.isEmpty()) {
                    writeThing(dce, gen, session, rows, batchSizeCounter, header);
                }
                appLogger.info("final # rows processed: " + totalRecordCounter);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeThing(DataConfigEntry dce, InsertGenerator gen, GraknClient.Session session, ArrayList<String> rows, int lineCounter, String header) throws IOException {
        int cores = dce.getThreads();
        try {
            if (isOfConceptType(dce.getProcessor(), "entity")) {
                ArrayList<Statement> insertStatements = gen.graknEntityInsert(rows, header);
                appLogger.trace("number of generated insert Statements: " + insertStatements.size());
                if (cores > 1) {
                    appLogger.debug("inserting using " + cores + " threads");
                    gm.futuresParallelInsertEntity(insertStatements, session, cores);
                } else {
                    appLogger.debug("inserting using 1 thread");
                    gm.insertEntityToGrakn(insertStatements, session);
                }
            } else {
                ArrayList<ArrayList<ArrayList<Statement>>> statements = gen.graknRelationInsert(rows, header);
                appLogger.trace("number of generated insert Statements: " + statements.get(0).size());
                if (cores > 1) {
                    appLogger.debug("inserting using " + cores + " threads");
                    gm.futuresParallelInsertRelation(statements, session, cores);
                } else {
                    appLogger.debug("inserting using 1 thread");
                    gm.insertRelationToGrakn(statements, session);
                }
            }
            updateMigrationStatusMigratedRows(dce, lineCounter);
        } catch (Exception ee) {
            ee.printStackTrace();
        }
    }

    private void clearMigrationStatusFile() throws IOException {
        new FileWriter(migrationStatePath, false).close();
    }

    private void getMigrationStatus() {
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

}
