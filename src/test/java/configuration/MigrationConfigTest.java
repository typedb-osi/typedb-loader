package configuration;

import org.junit.Assert;
import org.junit.Test;

import static util.Util.getAbsPath;

public class MigrationConfigTest {

    @Test
    public void dataAndGeneratorConfigTest() {
        String gcPath = getAbsPath("src/test/resources/genericTests/processorConfig-test.json");
        String dcPath = getAbsPath("src/test/resources/genericTests/dataConfig-test.json");

        MigrationConfig mg = new MigrationConfig("NA", "NA", "NA", dcPath, gcPath);

        // dataconfig tests
        Assert.assertEquals(8, mg.getDataConfig().get("entity1").getBatchSize());

        // generatorconfig tests
        Assert.assertEquals("entity1", mg.getProcessorConfig().get("processors").get(0).getProcessor());
        Assert.assertEquals("entity1-id", mg.getProcessorConfig().get("processors").get(0).getAttributeGenerator("entity1-id").getAttributeType());

//        Assert.assertNotNull(mg.getGeneratorConfig().get("entity1"));
//        Assert.assertNotNull(mg.getGeneratorConfig().get("rel1-source-1"));
//        Assert.assertTrue(mg.getGeneratorConfig().get("entity1") instanceof configuration.GeneratorConfig);
//        Assert.assertTrue(mg.getGeneratorConfig().get("rel1-source-1") instanceof configuration.GeneratorConfig);
//
//        Assert.assertEquals("entity", mg.getGeneratorConfig().get("entity1").getConfigEntry("entity1").getThingConcept());
//        Assert.assertEquals("entity", mg.getGeneratorConfig().get("entity1").getConfigEntry("entity1").getConceptType());
//        Assert.assertEquals("entity1", mg.getGeneratorConfig().get("entity1").getConfigEntry("entity1").getConceptName());
//        Assert.assertTrue(mg.getGeneratorConfig().get("entity1").getConfigEntry("entity1").isRequired());
//        Assert.assertEquals("entity-type", mg.getGeneratorConfig().get("entity1").getConfigEntry("entity1").getColumnName());
//
//        Assert.assertEquals("attribute", mg.getGeneratorConfig().get("entity1").getConfigEntry("entity1-exp").getThingConcept());
//        Assert.assertEquals("attribute", mg.getGeneratorConfig().get("entity1").getConfigEntry("entity1-exp").getConceptType());
//        Assert.assertEquals("entity1-exp", mg.getGeneratorConfig().get("entity1").getConfigEntry("entity1-exp").getConceptName());
//        Assert.assertFalse(mg.getGeneratorConfig().get("entity1").getConfigEntry("entity1-exp").isRequired());
//        Assert.assertEquals("entity1-exp", mg.getGeneratorConfig().get("entity1").getConfigEntry("entity1-exp").getColumnName());
//        Assert.assertEquals("###", mg.getGeneratorConfig().get("entity1").getConfigEntry("entity1-exp").getExplosionSeparator());
//
//        Assert.assertNull(mg.getGeneratorConfig().get("entity1").getConfigEntry("entity1-name").getExplosionSeparator());
    }
}

