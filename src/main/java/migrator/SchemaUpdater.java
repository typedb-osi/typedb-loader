package migrator;

import configuration.MigrationConfig;
import configuration.SchemaUpdateConfig;
import grakn.client.GraknClient;
import insert.GraknInserter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchemaUpdater {

    private final GraknInserter gm;
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");

    public SchemaUpdater(SchemaUpdateConfig suConfig) {
        this.gm = new GraknInserter(suConfig.getGraknURI().split(":")[0],
                suConfig.getGraknURI().split(":")[1],
                suConfig.getSchemaPath(),
                suConfig.getKeyspace()
        );
    }

    public void updateSchema() {
        GraknClient client = gm.getClient();
        GraknClient.Session session = gm.getSession(client);
        appLogger.info("applying schema to existing schema");
        gm.updateCurrentSchema(client, session);
        appLogger.info("GraMi is finished applying your schema!");
        session.close();
        client.close();
    }
}
