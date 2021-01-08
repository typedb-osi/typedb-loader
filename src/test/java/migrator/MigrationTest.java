package migrator;

import configuration.MigrationConfig;
import org.junit.Test;
import static util.Util.getAbsPath;

import java.io.IOException;

public class MigrationTest {

    @Test
    public void migrateGenericTestsTest() throws IOException {

        String keyspaceName = "grami_generic_test";
        String asp = getAbsPath("src/test/resources/genericTests/schema-test.gql");
        String msp = getAbsPath("src/test/resources/genericTests/migrationStatus-test.json");
        String adcp = getAbsPath("src/test/resources/genericTests/dataConfig-test.json");
        String gcp = getAbsPath("src/test/resources/genericTests/processorConfig-test.json");

        MigrationConfig migrationConfig = new MigrationConfig("localhost:48555",keyspaceName, asp, adcp, gcp);
        GraknMigrator mig = new GraknMigrator(migrationConfig, msp, true);
        mig.migrate(true, true);
    }

    @Test
    public void migratePhoneCallsTest() throws IOException {

        String keyspaceName = "grami_phone_call_test";
        String asp = getAbsPath("src/test/resources/phone-calls/schema.gql");
        String msp = getAbsPath("src/test/resources/phone-calls/migrationStatus.json");
        String adcp = getAbsPath("src/test/resources/phone-calls/dataConfig.json");
        String gcp = getAbsPath("src/test/resources/phone-calls/processorConfig.json");

        MigrationConfig migrationConfig = new MigrationConfig("localhost:48555",keyspaceName, asp, adcp, gcp);
        GraknMigrator mig = new GraknMigrator(migrationConfig, msp, true);
        mig.migrate(true, true);
    }

    @Test
    public void checkCorrectInsertPhoneCallsTest() throws IOException {

        String keyspaceName = "grami_phone_call_test";
        String asp = getAbsPath("src/test/resources/phone-calls/schema.gql");
        String msp = getAbsPath("src/test/resources/phone-calls/migrationStatus.json");
        String adcp = getAbsPath("src/test/resources/phone-calls/dataConfig.json");
        String gcp = getAbsPath("src/test/resources/phone-calls/processorConfig.json");

        MigrationConfig migrationConfig = new MigrationConfig("localhost:48555",keyspaceName, asp, adcp, gcp);
        GraknMigrator mig = new GraknMigrator(migrationConfig, msp, true);
        mig.migrate(true, true);

        //TODO
    }

    @Test
    public void issue10Test() throws IOException {

        String keyspaceName = "issue10";
        String asp = getAbsPath("src/test/resources/bugfixing/issue10/schema.gql");
        String msp = getAbsPath("src/test/resources/bugfixing/issue10/migrationStatus.json");
        String adcp = getAbsPath("src/test/resources/bugfixing/issue10/dataConfig.json");
        String gcp = getAbsPath("src/test/resources/bugfixing/issue10/processorConfig.json");

        MigrationConfig migrationConfig = new MigrationConfig("localhost:48555",keyspaceName, asp, adcp, gcp);
        GraknMigrator mig = new GraknMigrator(migrationConfig, msp, true);
        mig.migrate(true, true);

    }
}

