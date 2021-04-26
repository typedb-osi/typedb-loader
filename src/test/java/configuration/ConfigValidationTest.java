package configuration;

import write.TypeDBWriter;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static util.Util.getAbsPath;

public class ConfigValidationTest {

    @Test
    public void configValidationTest() {

        String schema = getAbsPath("src/test/resources/configValidation/schema.gql");
        TypeDBWriter gi = new TypeDBWriter("localhost", "1729", schema, "configValidation");
        gi.cleanAndDefineSchemaToDatabase(gi.getClient());

        // general tests
        String dataConf = getAbsPath("src/test/resources/configValidation/dataConfig.json");
        String procConf = getAbsPath("src/test/resources/configValidation/processorConfig.json");
        LoaderLoadConfig loaderLoadConfig = new LoaderLoadConfig("localhost:1729", "configValidation", schema, dataConf, procConf);
        HashMap<String, ArrayList<String>> reports = ConfigValidation.validateConfigs(loaderLoadConfig);
        Assert.assertEquals(2, reports.size());
        Assert.assertEquals(0, reports.get("processorConfig").size());
        Assert.assertEquals(0, reports.get("dataConfig").size());

        // processorType
        procConf = getAbsPath("src/test/resources/configValidation/processorConfig_processorType.json");
        loaderLoadConfig = new LoaderLoadConfig("localhost:1729", "configValidation", schema, dataConf, procConf);
        reports = ConfigValidation.validateConfigs(loaderLoadConfig);
        Assert.assertEquals(3, reports.get("processorConfig").size());




        // processorConfig phoneCalls
        procConf = getAbsPath("src/test/resources/phoneCalls/processorConfig.json");
        loaderLoadConfig = new LoaderLoadConfig("localhost:1729", "configValidation", schema, dataConf, procConf);
        reports = ConfigValidation.validateConfigs(loaderLoadConfig);
        Assert.assertEquals(0, reports.get("processorConfig").size());

    }
}
