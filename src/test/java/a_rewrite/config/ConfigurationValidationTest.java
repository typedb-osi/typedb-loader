package a_rewrite.config;

import a_rewrite.util.GraknUtil;
import a_rewrite.util.Util;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class ConfigurationValidationTest {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final String graknURI = "localhost:1729";
    private static final String databaseName = "1.0.0-config-validation-test";

    @Test
    public void validateNoSchemaDataConfig() {
        Configuration dc = Util.initializeDataConfig(new File("src/test/resources/1.0.0/synthetic/dcNoSchema.json").getAbsolutePath());
        assert dc != null;

        ConfigurationValidation cv = new ConfigurationValidation(dc);

        HashMap<String, ArrayList<String>> validationReport = new HashMap<>();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> warnings = new ArrayList<>();
        validationReport.put("warnings", warnings);
        validationReport.put("errors", errors);
        cv.validateSchemaPresent(validationReport);

        validationReport.get("warnings").forEach(appLogger::warn);
        validationReport.get("errors").forEach(appLogger::error);

        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "defaultConfig.schemaPath: missing required field"
        )));
    }

    @Test
    public void validateSchemaFileNotFoundDataConfig() {
        Configuration dc = Util.initializeDataConfig(new File("src/test/resources/1.0.0/synthetic/dcSchemaNotFound.json").getAbsolutePath());
        assert dc != null;

        ConfigurationValidation cv = new ConfigurationValidation(dc);

        HashMap<String, ArrayList<String>> validationReport = new HashMap<>();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> warnings = new ArrayList<>();
        validationReport.put("warnings", warnings);
        validationReport.put("errors", errors);
        cv.validateSchemaPresent(validationReport);

        validationReport.get("warnings").forEach(appLogger::warn);
        validationReport.get("errors").forEach(appLogger::error);

        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "defaultConfig.schemaPath - schema file not found under: <src/test/resources/1.0.0/synthetic/schema-not-found.gql>"
        )));
    }


    @Test
    public void validateErrorDataConfig() {
        Configuration dc = Util.initializeDataConfig(new File("src/test/resources/1.0.0/synthetic/dcValidationTest.json").getAbsolutePath());
        assert dc != null;

        TypeDBClient schemaClient = GraknUtil.getClient(graknURI, Runtime.getRuntime().availableProcessors());

        ConfigurationValidation cv = new ConfigurationValidation(dc);

        HashMap<String, ArrayList<String>> validationReport = new HashMap<>();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> warnings = new ArrayList<>();
        validationReport.put("warnings", warnings);
        validationReport.put("errors", errors);

        cv.validateSchemaPresent(validationReport);
        if (validationReport.get("errors").size() == 0) {
            GraknUtil.cleanAndDefineSchemaToDatabase(schemaClient, databaseName, dc.getDefaultConfig().getSchemaPath());
            appLogger.info("cleaned database and migrated schema...");
        }
        TypeDBSession schemaSession = GraknUtil.getSchemaSession(schemaClient, databaseName);
        cv.validateConfiguration(validationReport, schemaSession);
        schemaSession.close();
        schemaClient.close();
        validationReport.get("warnings").forEach(appLogger::warn);
        validationReport.get("errors").forEach(appLogger::error);

        // attributes
        Assert.assertTrue(validationReport.get("warnings").stream().anyMatch(message -> message.equals(
                "defaultConfig.rowsPerCommit is set to be > 150 - in most cases, choosing a value between 50 and 150 gives the best performance"
        )));

        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "attributes.names.column: <doesnotexist> column not found in header of file <src/test/resources/1.0.0/synthetic/names.csv>"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "attributes.no-file.dataPath: <src/test/resources/1.0.0/synthetic/nam.csv>: file not found"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "attributes.no-file.dataPath: <src/test/resources/1.0.0/synthetic/na.csv>: file not found"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "attributes.no-file.conceptType: <doesnotexist> does not exist in schema"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "attributes.empty-file.dataPath: <src/test/resources/1.0.0/synthetic/empty.csv>: file is empty"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "attributes.empty-file.dataPath: <src/test/resources/1.0.0/synthetic/nam.csv>: file not found"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "attributes.empty-file.conceptType: <doesnotexist> does not exist in schema"
        )));

        //entities
        Assert.assertTrue(validationReport.get("warnings").stream().anyMatch(message -> message.equals(
                "entities.person.attributes.[3].requireNonEmpty: field not set - defaults to false"
        )));

        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "entities.missing-attributes.attributes: missing required attributes block"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "entities.person.dataPath: <src/test/resources/1.0.0/synthetic/empty.csv>: file is empty"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "entities.person.dataPath: <src/test/resources/1.0.0/synthetic/notfound.csv>: file not found"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "entities.person.dataPath: <src/test/resources/1.0.0/synthetic/notfound-other.csv>: file not found"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "entities.person.conceptType: <doesnotexist> does not exist in schema"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "entities.person.attributes.[1].conceptType: <doesnotexist> does not exist in schema"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "entities.person.attributes.[2].column: <doesnotexist> column not found in header of file <src/test/resources/1.0.0/synthetic/names.csv>"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "entities.person.attributes.[4].conceptType: missing required field"
        )));
        Assert.assertTrue(validationReport.get("errors").stream().anyMatch(message -> message.equals(
                "entities.person.attributes.[5].column: missing required field"
        )));

        // overall
        Assert.assertEquals(2, validationReport.get("warnings").size());
        Assert.assertEquals(16, validationReport.get("errors").size());
    }

    @Test
    public void validateErrorDataConfigPhoneCalls() {
        Configuration dc = Util.initializeDataConfig(new File("src/test/resources/1.0.0/phoneCalls/dcValidationTest.json").getAbsolutePath());
        assert dc != null;

        TypeDBClient schemaClient = GraknUtil.getClient(graknURI, Runtime.getRuntime().availableProcessors());

        ConfigurationValidation cv = new ConfigurationValidation(dc);

        HashMap<String, ArrayList<String>> validationReport = new HashMap<>();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> warnings = new ArrayList<>();
        validationReport.put("warnings", warnings);
        validationReport.put("errors", errors);

        cv.validateSchemaPresent(validationReport);
        if (validationReport.get("errors").size() == 0) {
            GraknUtil.cleanAndDefineSchemaToDatabase(schemaClient, databaseName, dc.getDefaultConfig().getSchemaPath());
            appLogger.info("cleaned database and migrated schema...");
        }
        TypeDBSession schemaSession = GraknUtil.getSchemaSession(schemaClient, databaseName);
        cv.validateConfiguration(validationReport, schemaSession);
        schemaSession.close();
        schemaClient.close();
        validationReport.get("warnings").forEach(appLogger::warn);
        validationReport.get("errors").forEach(appLogger::error);

        //TODO: cover all cases from synthetic, remove synthetic, and add all test cases for relations
    }

    @Test
    public void validateDataConfig() {
        Configuration dc = Util.initializeDataConfig(new File("src/test/resources/1.0.0/synthetic/dc.json").getAbsolutePath());
        assert dc != null;
        TypeDBClient schemaClient = GraknUtil.getClient(graknURI, Runtime.getRuntime().availableProcessors());

        ConfigurationValidation cv = new ConfigurationValidation(dc);

        HashMap<String, ArrayList<String>> validationReport = new HashMap<>();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> warnings = new ArrayList<>();
        validationReport.put("warnings", warnings);
        validationReport.put("errors", errors);
        cv.validateSchemaPresent(validationReport);

        if (validationReport.get("errors").size() == 0) {
            GraknUtil.cleanAndDefineSchemaToDatabase(schemaClient, databaseName, dc.getDefaultConfig().getSchemaPath());
            appLogger.info("cleaned database and migrated schema...");
        }

        TypeDBSession schemaSession = GraknUtil.getSchemaSession(schemaClient, databaseName);
        cv.validateConfiguration(validationReport, schemaSession);
        schemaSession.close();
        schemaClient.close();

        validationReport.get("warnings").forEach(appLogger::warn);
        validationReport.get("errors").forEach(appLogger::error);
        Assert.assertEquals(0, validationReport.get("warnings").size());
        Assert.assertEquals(0, validationReport.get("errors").size());
    }

    @Test
    public void validatePhoneCallsDataConfig() {
        Configuration dc = Util.initializeDataConfig(new File("src/test/resources/1.0.0/phoneCalls/dc.json").getAbsolutePath());
        assert dc != null;

        TypeDBClient schemaClient = GraknUtil.getClient(graknURI, Runtime.getRuntime().availableProcessors());
        ConfigurationValidation cv = new ConfigurationValidation(dc);

        HashMap<String, ArrayList<String>> validationReport = new HashMap<>();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> warnings = new ArrayList<>();
        validationReport.put("warnings", warnings);
        validationReport.put("errors", errors);
        cv.validateSchemaPresent(validationReport);
        if (validationReport.get("errors").size() == 0) {
            GraknUtil.cleanAndDefineSchemaToDatabase(schemaClient, databaseName, dc.getDefaultConfig().getSchemaPath());
            appLogger.info("cleaned database and migrated schema...");
        }

        TypeDBSession schemaSession = GraknUtil.getSchemaSession(schemaClient, databaseName);
        cv.validateConfiguration(validationReport, schemaSession);
        schemaSession.close();
        schemaClient.close();

        validationReport.get("warnings").forEach(appLogger::warn);
        validationReport.get("errors").forEach(appLogger::error);

        Assert.assertEquals(0, validationReport.get("warnings").size());
        Assert.assertEquals(0, validationReport.get("errors").size());
    }
}
