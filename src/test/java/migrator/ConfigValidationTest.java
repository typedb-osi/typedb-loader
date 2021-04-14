package migrator;

import configuration.MigrationConfig;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static util.Util.getAbsPath;

public class ConfigValidationTest {

    @Test
    public void migrateGenericTestsTest() throws IOException {

        String schema = getAbsPath("src/test/resources/genericTests/schema-test.gql");
        String dataconf = getAbsPath("src/test/resources/config-validation/dataConfig.json");
        String procconf = getAbsPath("src/test/resources/config-validation/processorConfig.json");

        MigrationConfig migrationConfig = new MigrationConfig("localhost:1729", "someDB", schema, dataconf, procconf);
        ArrayList<ValidationReport> reports = ConfigValidation.validateConfigs(migrationConfig);

        for (ValidationReport rep : reports) {
            for (String s : rep.getErrors()) {
                System.out.println(s);
            }
        }
    }
}
