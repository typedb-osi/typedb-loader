package configuration;

import grakn.client.api.GraknClient;
import grakn.client.api.GraknSession;
import grakn.client.api.GraknTransaction;
import grakn.client.api.concept.type.AttributeType;
import graql.lang.Graql;
import graql.lang.query.GraqlMatch;
import write.TypeDBWriter;
import processor.InsertProcessor;

import java.util.List;
import java.util.stream.Collectors;

import static graql.lang.Graql.var;

public class EntryMigrationConfig {
    private final ConfigEntryData dce;
    private final ConfigEntryProcessor pce;
    private final int dataPathIndex;
    private final String migrationStatusKey;
    private final Integer migratedRows;
    private final InsertProcessor insertProcessor;
    private final String schemaTypeKey;

    public EntryMigrationConfig(ConfigEntryData dce, ConfigEntryProcessor pce, int dataPathIndex, String migrationStatusKey, Integer migratedRows, InsertProcessor insertProcessor, TypeDBWriter typeDBWriter) {
        this.dce = dce;
        this.pce = pce;
        this.dataPathIndex = dataPathIndex;
        this.migrationStatusKey = migrationStatusKey;
        this.migratedRows = migratedRows;
        this.insertProcessor = insertProcessor;
        this.schemaTypeKey = getSchemaTypeKey(pce.getSchemaType(), typeDBWriter);
    }

    public String getSchemaTypeKey() {
        return schemaTypeKey;
    }

    public ConfigEntryData getDce() {
        return dce;
    }

    public ConfigEntryProcessor getPce() {
        return pce;
    }

    public int getDataPathIndex() {
        return dataPathIndex;
    }

    public String getMigrationStatusKey() {
        return migrationStatusKey;
    }

    public Integer getMigratedRows() {
        return migratedRows;
    }

    public InsertProcessor getInsertGenerator() {
        return insertProcessor;
    }

    private String getSchemaTypeKey(String schemaType, TypeDBWriter typeDBWriter) {
        final String[] key = {null};
        GraknClient client = typeDBWriter.getClient();
        GraknSession session = typeDBWriter.getSchemaSession(client);
        GraknTransaction tx = session.transaction(GraknTransaction.Type.READ);

        GraqlMatch getQuery = Graql.match(var("st").type(schemaType)).get("st").limit(1000);
        tx.query().match(getQuery).forEach(answers -> answers.concepts().forEach(entry -> {
            List<AttributeType> keys = entry.asRemote(tx).asThingType().getOwns(true).collect(Collectors.toList());
            if (keys.size() > 0) {
                key[0] = keys.get(0).getLabel().name();
            }
        }));
        tx.close();

        session.close();
        client.close();
        return key[0];
    }
}
