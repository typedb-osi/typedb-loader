package migrator;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import generator.InsertGenerator;
import grakn.client.api.GraknClient;
import grakn.client.api.GraknSession;
import grakn.client.api.GraknTransaction;
import grakn.client.api.concept.type.AttributeType;
import graql.lang.Graql;
import graql.lang.query.GraqlMatch;
import insert.GraknInserter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static graql.lang.Graql.var;

public class EntryMigrationConfig {
    private final DataConfigEntry dce;
    private final ProcessorConfigEntry pce;
    private final int dataPathIndex;
    private final String migrationStatusKey;
    private final Integer migratedRows;
    private final InsertGenerator insertGenerator;
    private final String schemaTypeKey;

    public EntryMigrationConfig(DataConfigEntry dce, ProcessorConfigEntry pce, int dataPathIndex, String migrationStatusKey, Integer migratedRows, InsertGenerator insertGenerator, GraknInserter graknInserter) {
        this.dce = dce;
        this.pce = pce;
        this.dataPathIndex = dataPathIndex;
        this.migrationStatusKey = migrationStatusKey;
        this.migratedRows = migratedRows;
        this.insertGenerator = insertGenerator;
        this.schemaTypeKey = getSchemaTypeKey(pce.getSchemaType(), graknInserter);
    }

    public String getSchemaTypeKey() { return schemaTypeKey; }

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

    private String getSchemaTypeKey(String schemaType, GraknInserter graknInserter) {
        final String[] key = {null};
        GraknClient client = graknInserter.getClient();
        GraknSession session = graknInserter.getSchemaSession(client);
        GraknTransaction tx = session.transaction(GraknTransaction.Type.READ);

        GraqlMatch getQuery = Graql.match(var("st").type(schemaType)).get("st").limit(1000);
        tx.query().match(getQuery).forEach( answers -> {
            answers.concepts().forEach(entry -> {
                List<AttributeType> keys = entry.asRemote(tx).asThingType().getOwns(true).collect(Collectors.toList());
                if (keys.size() > 0) {
                    key[0] = keys.get(0).getLabel().name();
                }
            });
        });
        tx.close();

        session.close();
        client.close();
        return key[0];
    }
}
