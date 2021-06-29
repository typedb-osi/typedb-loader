package configuration;

import com.vaticle.typedb.client.api.concept.type.AttributeType;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import write.TypeDBWriter;
import processor.InsertProcessor;

import java.util.List;
import java.util.stream.Collectors;

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
        TypeDBClient client = typeDBWriter.getClient();
        TypeDBSession session = typeDBWriter.getSchemaSession(client);
        TypeDBTransaction tx = session.transaction(TypeDBTransaction.Type.READ);

        TypeQLMatch getQuery = TypeQL.match(TypeQL.var("st").type(schemaType)).get("st").limit(1000);
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
