package configuration;

import org.junit.Assert;
import org.junit.Test;
import processor.ProcessorType;

import static util.Util.getAbsPath;

public class MigrationConfigTest {

    @Test
    public void dataAndGeneratorConfigTest() {
        String gcPath = getAbsPath("src/test/resources/genericTests/processorConfig-test.json");
        String dcPath = getAbsPath("src/test/resources/genericTests/dataConfig-test.json");

        MigrationConfig mg = new MigrationConfig(null, null, null, dcPath, gcPath);

        // dataconfig tests
        Assert.assertEquals(8, mg.getDataConfig().get("entity1").getBatchSize());
        Assert.assertEquals(4, mg.getDataConfig().get("entity1").getThreads());
        Assert.assertEquals(3, mg.getDataConfig().get("entity1").getAttributeProcessorMappings().length);
        Assert.assertEquals("entity1", mg.getDataConfig().get("entity1").getProcessor());
        Assert.assertEquals('\t', mg.getDataConfig().get("entity1").getSeparator().charValue());
        Assert.assertEquals(1, mg.getDataConfig().get("entity1").getDataPath().length);
        Assert.assertNull(mg.getDataConfig().get("entity1").getAttributeProcessorMappings()[0].getListSeparator());
        Assert.assertEquals("###", mg.getDataConfig().get("entity1").getAttributeProcessorMappings()[1].getListSeparator());

        // generatorconfig tests
        Assert.assertEquals("entity1", mg.getProcessorConfig().get("processors").get(0).getProcessor());
        Assert.assertEquals(ProcessorType.ENTITY, mg.getProcessorConfig().get("processors").get(0).getProcessorType());
        Assert.assertEquals("entity1", mg.getProcessorConfig().get("processors").get(0).getSchemaType());
        Assert.assertEquals(3, mg.getProcessorConfig().get("processors").get(0).getAttributes().size());
        Assert.assertEquals("entity1-id", mg.getProcessorConfig().get("processors").get(0).getAttributeGenerator("entity1-id").getAttributeType());
    }
}

