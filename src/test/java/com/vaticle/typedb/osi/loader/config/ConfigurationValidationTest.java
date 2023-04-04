/*
 * Copyright (C) 2021 Bayer AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.vaticle.typedb.osi.loader.config;

import com.vaticle.typedb.client.api.TypeDBClient;
import com.vaticle.typedb.client.api.TypeDBSession;
import com.vaticle.typedb.osi.loader.util.TypeDBUtil;
import com.vaticle.typedb.osi.loader.util.Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class ConfigurationValidationTest {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.tdl.loader");
    private static final String typedbURI = "localhost:1729";
    private static final String databaseName = "config-validation-test";

    @Test
    public void validateNoSchemaDataConfig() {
        Configuration dc = Util.initializeConfig(new File("src/test/resources/generic/configNoSchema.json").getAbsolutePath());
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
                "defaultConfig.schema: missing required field"
        )));
    }

    @Test
    public void validateSchemaFileNotFoundDataConfig() {
        Configuration dc = Util.initializeConfig(new File("src/test/resources/generic/configSchemaNotFound.json").getAbsolutePath());
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
                "defaultConfig.schema - schema file not found under: <src/test/resources/synthetic/schema-not-found.gql>"
        )));
    }


    @Test
    public void validateErrorDataConfig() {
        Configuration dc = Util.initializeConfig(new File("src/test/resources/generic/configValidationTest.json").getAbsolutePath());
        assert dc != null;

        TypeDBClient schemaClient = TypeDBUtil.getCoreClient(typedbURI, Runtime.getRuntime().availableProcessors());

        ConfigurationValidation cv = new ConfigurationValidation(dc);

        HashMap<String, ArrayList<String>> validationReport = new HashMap<>();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> warnings = new ArrayList<>();
        validationReport.put("warnings", warnings);
        validationReport.put("errors", errors);

        cv.validateSchemaPresent(validationReport);
        if (validationReport.get("errors").size() == 0) {
            TypeDBUtil.cleanAndDefineSchemaToDatabase(schemaClient, databaseName, dc.getGlobalConfig().getSchema());
            appLogger.info("cleaned database and migrated schema...");
        }
        TypeDBSession schemaSession = TypeDBUtil.getSchemaSession(schemaClient, databaseName);
        cv.validateConfiguration(validationReport, schemaSession);
        schemaSession.close();
        schemaClient.close();
        validationReport.get("warnings").forEach(appLogger::warn);
        validationReport.get("errors").forEach(appLogger::error);

        // overall
        Assert.assertEquals(2, validationReport.get("warnings").size());
        Assert.assertEquals(10, validationReport.get("errors").size());
    }

    @Test
    public void validateErrorDataConfigPhoneCalls() {
        Configuration dc = Util.initializeConfig(new File("src/test/resources/phoneCalls/configValidationTest.json").getAbsolutePath());
        assert dc != null;

        TypeDBClient schemaClient = TypeDBUtil.getCoreClient(typedbURI, Runtime.getRuntime().availableProcessors());

        ConfigurationValidation cv = new ConfigurationValidation(dc);

        HashMap<String, ArrayList<String>> validationReport = new HashMap<>();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> warnings = new ArrayList<>();
        validationReport.put("warnings", warnings);
        validationReport.put("errors", errors);

        cv.validateSchemaPresent(validationReport);
        if (validationReport.get("errors").size() == 0) {
            TypeDBUtil.cleanAndDefineSchemaToDatabase(schemaClient, databaseName, dc.getGlobalConfig().getSchema());
            appLogger.info("cleaned database and migrated schema...");
        }
        TypeDBSession schemaSession = TypeDBUtil.getSchemaSession(schemaClient, databaseName);
        cv.validateConfiguration(validationReport, schemaSession);
        schemaSession.close();
        schemaClient.close();
        validationReport.get("warnings").forEach(appLogger::warn);
        validationReport.get("errors").forEach(appLogger::error);

        //TODO: cover all cases from synthetic, remove synthetic, and add all test cases for relations
    }

    @Test
    public void validateDataConfig() {
        Configuration dc = Util.initializeConfig(new File("src/test/resources/generic/config.json").getAbsolutePath());
        assert dc != null;
        TypeDBClient schemaClient = TypeDBUtil.getCoreClient(typedbURI, Runtime.getRuntime().availableProcessors());

        ConfigurationValidation cv = new ConfigurationValidation(dc);

        HashMap<String, ArrayList<String>> validationReport = new HashMap<>();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> warnings = new ArrayList<>();
        validationReport.put("warnings", warnings);
        validationReport.put("errors", errors);
        cv.validateSchemaPresent(validationReport);

        if (validationReport.get("errors").size() == 0) {
            TypeDBUtil.cleanAndDefineSchemaToDatabase(schemaClient, databaseName, dc.getGlobalConfig().getSchema());
            appLogger.info("cleaned database and migrated schema...");
        }

        TypeDBSession schemaSession = TypeDBUtil.getSchemaSession(schemaClient, databaseName);
        cv.validateConfiguration(validationReport, schemaSession);
        schemaSession.close();
        schemaClient.close();

        validationReport.get("warnings").forEach(appLogger::warn);
        validationReport.get("errors").forEach(appLogger::error);
        Assert.assertEquals(1, validationReport.get("warnings").size());
        Assert.assertEquals(0, validationReport.get("errors").size());
    }

    @Test
    public void validatePhoneCallsDataConfig() {
        Configuration dc = Util.initializeConfig(new File("src/test/resources/phoneCalls/config.json").getAbsolutePath());
        assert dc != null;

        TypeDBClient schemaClient = TypeDBUtil.getCoreClient(typedbURI, Runtime.getRuntime().availableProcessors());
        ConfigurationValidation cv = new ConfigurationValidation(dc);

        HashMap<String, ArrayList<String>> validationReport = new HashMap<>();
        ArrayList<String> errors = new ArrayList<>();
        ArrayList<String> warnings = new ArrayList<>();
        validationReport.put("warnings", warnings);
        validationReport.put("errors", errors);
        cv.validateSchemaPresent(validationReport);
        if (validationReport.get("errors").size() == 0) {
            TypeDBUtil.cleanAndDefineSchemaToDatabase(schemaClient, databaseName, dc.getGlobalConfig().getSchema());
            appLogger.info("cleaned database and migrated schema...");
        }

        TypeDBSession schemaSession = TypeDBUtil.getSchemaSession(schemaClient, databaseName);
        cv.validateConfiguration(validationReport, schemaSession);
        schemaSession.close();
        schemaClient.close();

        validationReport.get("warnings").forEach(appLogger::warn);
        validationReport.get("errors").forEach(appLogger::error);

        Assert.assertEquals(7, validationReport.get("warnings").size());
        Assert.assertEquals(0, validationReport.get("errors").size());
    }
}
