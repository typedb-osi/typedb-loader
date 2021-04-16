package migrator;

import configuration.MigrationConfig;
import insert.GraknInserter;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static util.Util.getAbsPath;

public class ConfigValidationTest {

    @Test
    public void configValidationTest() {

        String schema = getAbsPath("src/test/resources/config-validation/schema.gql");
        GraknInserter gi = new GraknInserter("localhost", "1729", schema, "config-validation");
        gi.cleanAndDefineSchemaToDatabase(gi.getClient());

        // general tests
        String dataConf = getAbsPath("src/test/resources/config-validation/dataConfig.json");
        String procConf = getAbsPath("src/test/resources/config-validation/processorConfig.json");
        MigrationConfig migrationConfig = new MigrationConfig("localhost:1729", "config-validation", schema, dataConf, procConf);
        HashMap<String, ArrayList<String>> reports = ConfigValidation.validateConfigs(migrationConfig);
        Assert.assertEquals(2, reports.size());
        Assert.assertEquals(0, reports.get("processorConfig").size());
        Assert.assertEquals(0, reports.get("dataConfig").size());

        // processorType
        procConf = getAbsPath("src/test/resources/config-validation/processorConfig_processorType.json");
        migrationConfig = new MigrationConfig("localhost:1729", "config-validation", schema, dataConf, procConf);
        reports = ConfigValidation.validateConfigs(migrationConfig);
        Assert.assertEquals(3, reports.get("processorConfig").size());




        // processorConfig phone-calls
        procConf = getAbsPath("src/test/resources/phone-calls/processorConfig.json");
        migrationConfig = new MigrationConfig("localhost:1729", "config-validation", schema, dataConf, procConf);
        reports = ConfigValidation.validateConfigs(migrationConfig);
        Assert.assertEquals(0, reports.get("processorConfig").size());

    }
}
