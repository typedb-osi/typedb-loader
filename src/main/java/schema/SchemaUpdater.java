package schema;

import configuration.SchemaUpdateConfig;
import grakn.client.api.GraknClient;
import write.TypeDBWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchemaUpdater {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private final TypeDBWriter gm;

    public SchemaUpdater(SchemaUpdateConfig suConfig) {
        this.gm = new TypeDBWriter(suConfig.getGraknURI().split(":")[0],
                suConfig.getGraknURI().split(":")[1],
                suConfig.getSchemaPath(),
                suConfig.getKeyspace()
        );
    }

    public void updateSchema() {
        GraknClient client = gm.getClient();
        appLogger.info("applying schema to existing schema");
        gm.loadAndDefineSchema(client);
        appLogger.info("GraMi is finished applying your schema!");
        client.close();
    }
}
