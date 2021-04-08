package generator;

import configuration.MigrationConfig;
import configuration.ProcessorConfigEntry;
import graql.lang.pattern.variable.ThingVariable;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static test.TestUtil.getData;
import static util.Util.getAbsPath;

public class AttributeInsertGeneratorTest {

    private final String adcp = getAbsPath("src/test/resources/phone-calls/dataConfig.json");
    private final String gcp = getAbsPath("src/test/resources/phone-calls/processorConfig.json");
    private final String file = getAbsPath("src/test/resources/phone-calls/is-in-use.csv");
    private final MigrationConfig migrationConfig = new MigrationConfig("localhost:1729", "null", "null", adcp, gcp);
    private final HashMap<String, ArrayList<ProcessorConfigEntry>> genConf = migrationConfig.getProcessorConfig();

    @Test
    public void graknAttributeQueryFromRowTest() {

        AttributeInsertGenerator testAttributeInsertGenerator = new AttributeInsertGenerator(migrationConfig.getDataConfig().get("is-in-use"), genConf.get("processors").get(8));

        ArrayList<String> rows = getData(file);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));

        ArrayList<ThingVariable<?>> result = testAttributeInsertGenerator.graknAttributeInsert(rows, header, 1);

        String tc0 = "$a \"yes\" isa is-in-use";
        Assert.assertEquals(tc0, result.get(0).toString());

        String tc1 = "$a \"no\" isa is-in-use";
        Assert.assertEquals(tc1, result.get(1).toString());

        Assert.assertEquals(2, result.size());
    }
}
