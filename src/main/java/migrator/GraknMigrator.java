package migrator;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import configuration.*;
import dataLoader.DataLoader;
import grakn.client.GraknClient;
import insert.GraknInserter;
import graql.lang.statement.Statement;
import queryGenerator.EntityInsertGenerator;
import queryGenerator.InsertGenerator;

import java.io.*;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import queryGenerator.RelationInsertGenerator;


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
        for (String key : dataConfig.keySet()) {
            if(isOfConceptType(key, conceptType)){
                appLogger.info("migrating [" + key + "]...");
                if (migrationStatus != null && migrationStatus.get(key) != null) {
                    appLogger.info("previous migration status found for entity type: [" + key + "]");
                    if (!migrationStatus.get(key).isCompleted()) {
                        appLogger.info(key + " not completely migrated yet, rows already migrated: " + migrationStatus.get(key).getMigratedRows());
                        getGeneratorAndInsert(session, key, conceptType, migrationStatus.get(key).getMigratedRows());
                    } else {
                        appLogger.info(key + " is already completely migrated - moving on...");
                    }
                } else {
                    appLogger.info("nothing previously migrated for [" + key + "] - starting with row 0");
                    getGeneratorAndInsert(session,  key, conceptType, 0);
                }
            }
        }
    }

    private boolean isOfConceptType(String key, String conceptType) {
        for (ProcessorConfigEntry gce : migrationConfig.getGeneratorConfig().get("processors")) {
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

    private void getGeneratorAndInsert(GraknClient.Session session, String key, String conceptType, int skipRows) throws IOException {
        // choose insert generator
        InsertGenerator gen = null;
        if (conceptType.contentEquals("entity")) {
            gen = getProcessor(key);
        } else if (conceptType.contentEquals("relation")) {
            gen = getProcessor(key);
        }

        writeThingToGrakn(key, gen, session, skipRows);
        updateMigrationStatusIsCompleted(key);
    }

    private void writeThingToGrakn(String key, InsertGenerator gen, GraknClient.Session session, int skipLines) {
        InputStream entityStream = DataLoader.getInputStream(dataConfig.get(key).getDataPath());
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
                    if (batchSizeCounter == dataConfig.get(key).getBatchSize()) {
                        writeThing(key, gen, session, rows, batchSizeCounter, header);
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
                    writeThing(key, gen, session, rows, batchSizeCounter, header);
                }
                appLogger.info("final # rows processed: " + totalRecordCounter);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeThing(String key, InsertGenerator gen, GraknClient.Session session, ArrayList<String> rows, int lineCounter, String header) throws IOException {
        int cores = dataConfig.get(key).getThreads();
        try {
            if (isOfConceptType(key, "entity")) {
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
            updateMigrationStatusMigratedRows(key, lineCounter);
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

    private void updateMigrationStatusMigratedRows(String key, int lineCounter) throws IOException {
        try {
            Gson gson = new Gson();
            Type MigrationStatusMapType = new TypeToken<HashMap<String, MigrationStatus>>(){}.getType();

            if (migrationStatus != null) {
                if (migrationStatus.get(key) != null) { //updating an existing entry
                    int updatedMigratedRows = migrationStatus.get(key).getMigratedRows() + lineCounter;
                    migrationStatus.get(key).setMigratedRows(updatedMigratedRows);
                } else { // writing new entry
                    migrationStatus.put(key, new MigrationStatus(key, false, lineCounter));
                }
            } else { //writing very first entry (i.e. file was empty)
                migrationStatus = new HashMap<>();
                migrationStatus.put(key, new MigrationStatus(key, false, lineCounter));
            }

            // update file
            FileWriter fw = new FileWriter(migrationStatePath);
            gson.toJson(migrationStatus, MigrationStatusMapType, fw);
            fw.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void updateMigrationStatusIsCompleted(String key) throws IOException {
        try {
            Gson gson = new Gson();
            Type MigrationStatusMapType = new TypeToken<HashMap<String, MigrationStatus>>(){}.getType();
            migrationStatus.get(key).setCompleted(true);
            FileWriter fw = new FileWriter(migrationStatePath);
            gson.toJson(migrationStatus, MigrationStatusMapType, fw);
            fw.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private InsertGenerator getProcessor(String key) {
        DataConfigEntry dce = dataConfig.get(key);
        ProcessorConfigEntry gce = getGenFromGenConfig(dce.getProcessor(), migrationConfig.getGeneratorConfig());

        if (gce != null && isOfConceptType(key, "entity")) {
            appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
            return new EntityInsertGenerator(dce, gce);
        } else if (gce != null && isOfConceptType(key, "relation")) {
            appLogger.debug("selected generator: " + gce.getProcessor() + " of type: " + gce.getProcessorType() + " based on dataConfig.generator: " + dce.getProcessor());
            return new RelationInsertGenerator(dce, gce);
        } else {
            throw new IllegalArgumentException(String.format("Invalid/No generator provided for: %s", dce.getProcessor()));
        }
    }

    private ProcessorConfigEntry getGenFromGenConfig(String generator, HashMap<String, ArrayList<ProcessorConfigEntry>> processorConfig) {
        for (ProcessorConfigEntry e : processorConfig.get("processors")) {
            if (e.getProcessor().equals(generator)) {
                return e;
            }
        }
        return null;
    }

}
