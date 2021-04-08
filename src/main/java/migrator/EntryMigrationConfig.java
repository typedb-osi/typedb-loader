package migrator;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import generator.InsertGenerator;

public class EntryMigrationConfig {
    private final DataConfigEntry dce;
    private final ProcessorConfigEntry pce;
    private final int dataPathIndex;
    private final String migrationStatusKey;
    private final Integer migratedRows;
    private final InsertGenerator insertGenerator;

    public EntryMigrationConfig(DataConfigEntry dce, ProcessorConfigEntry pce, int dataPathIndex, String migrationStatusKey, Integer migratedRows, InsertGenerator insertGenerator) {
        this.dce = dce;
        this.pce = pce;
        this.dataPathIndex = dataPathIndex;
        this.migrationStatusKey = migrationStatusKey;
        this.migratedRows = migratedRows;
        this.insertGenerator = insertGenerator;
    }

    public DataConfigEntry getDce() {
        return dce;
    }

    public ProcessorConfigEntry getPce() {
        return pce;
    }

    public int getDataPathIndex() { return dataPathIndex; }

    public String getMigrationStatusKey() {
        return migrationStatusKey;
    }

    public Integer getMigratedRows() {
        return migratedRows;
    }

    public InsertGenerator getInsertGenerator() {
        return insertGenerator;
    }
}
