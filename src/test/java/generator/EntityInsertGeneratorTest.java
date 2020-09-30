package generator;

import configuration.MigrationConfig;
import configuration.ProcessorConfigEntry;
import graql.lang.statement.Statement;
import org.junit.Assert;
import org.junit.Test;
import static util.Util.getAbsPath;
import java.util.ArrayList;
import java.util.HashMap;

import static test.TestUtil.getData;

public class EntityInsertGeneratorTest {

    private final String keyspaceName = "grakn_migrator_test";
    private final String asp = getAbsPath("src/test/resources/genericTests/schema-test.gql");
    private final String adcp = getAbsPath("src/test/resources/genericTests/dataConfig-test.json");
    private final String gcp = getAbsPath("src/test/resources/genericTests/processorConfig-test.json");
    private final String entity1dp = getAbsPath("src/test/resources/genericTests/entity1-test-data.tsv");
    private final String entity2dp = getAbsPath("src/test/resources/genericTests/entity2-test-data.tsv");
    private final String entity3dp = getAbsPath("src/test/resources/genericTests/entity3-test-data.tsv");
    private final MigrationConfig migrationConfig = new MigrationConfig("localhost:48555",keyspaceName, asp, adcp, gcp);
    private final HashMap<String, ArrayList<ProcessorConfigEntry>> genConf = migrationConfig.getProcessorConfig();

    @Test
    public void graknEntityQueryFromRowTest() {

        EntityInsertGenerator testEntityInsertGenerator = new EntityInsertGenerator(migrationConfig.getDataConfig().get("entity1"), genConf.get("processors").get(0));

        ArrayList<String> rows = getData(entity1dp);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));

        ArrayList < Statement > result = testEntityInsertGenerator.graknEntityInsert(rows, header);

        String tc0 = "$e-0 isa entity1, has entity1-id \"entity1id0\", has entity1-name \"entity1name0\", has entity1-exp \"entity1id0exp0\";";
        Assert.assertEquals(tc0, result.get(0).toString());

        String tc1 = "$e-1 isa entity1, has entity1-id \"entity1id1\", has entity1-name \"entity1name1\", has entity1-exp \"entity1id1exp11\", has entity1-exp \"entity1id1exp12\";";
        Assert.assertEquals(tc1, result.get(1).toString());

        String tc2 = "$e-2 isa entity1, has entity1-id \"entity1id2\", has entity1-name \"entity1name2\", has entity1-exp \"entity1id2exp21\", has entity1-exp \"entity1id2exp22\", has entity1-exp \"entity1id2exp23\";";
        Assert.assertEquals(tc2, result.get(2).toString());

        String tc3 = "$e-3 isa entity1, has entity1-id \"entity1id3\", has entity1-name \"entity1name3\";";
        Assert.assertEquals(tc3, result.get(3).toString());

        String tc4 = "$e-4 isa entity1, has entity1-id \"entity1id4\", has entity1-name \"entity1name4\";";
        Assert.assertEquals(tc4, result.get(4).toString());

        String tc5 = "$e-5 isa entity1, has entity1-id \"entity1id5\", has entity1-name \"entity1name5\";";
        Assert.assertEquals(tc5, result.get(5).toString());

        String tc6 = "$e-6 isa entity1, has entity1-id \"entity1id6\", has entity1-name \"entity1name6\";";
        Assert.assertEquals(tc6, result.get(6).toString());

        String tc7 = "$e-7 isa entity1, has entity1-id \"entity1id7\", has entity1-name \"entity1name7\";";
        Assert.assertEquals(tc7, result.get(7).toString());

        String tc8 = "$e-8 isa entity1, has entity1-id \"entity1id8\", has entity1-name \"entity1name8\";";
        Assert.assertEquals(tc8, result.get(8).toString());

        String tc9 = "$e-9 isa entity1, has entity1-id \"entity1id9\", has entity1-name \"entity1name9\";";
        Assert.assertEquals(tc9, result.get(9).toString());

        String tc10 = "$e-10 isa entity1, has entity1-id \"entity1id10\", has entity1-name \"entity1name10\";";
        Assert.assertEquals(tc10, result.get(10).toString());

        String tc11 = "$e-11 isa entity1, has entity1-id \"entity1id11\", has entity1-name \"entity1name11\";";
        Assert.assertEquals(tc11, result.get(11).toString());

        String tc12 = "$e-12 isa entity1, has entity1-id \"entity1id12\", has entity1-name \"entity1name12\";";
        Assert.assertEquals(tc12, result.get(12).toString());

        String tc13 = "$e-13 isa entity1, has entity1-id \"entity1id13\", has entity1-name \"entity1name13\";";
        Assert.assertEquals(tc13, result.get(13).toString());

        String tc14 = "$e-14 isa entity1, has entity1-id \"entity1id14\", has entity1-name \"entity1name14\";";
        Assert.assertEquals(tc14, result.get(14).toString());

        String tc15 = "$e-15 isa entity1, has entity1-id \"entity1id15\", has entity1-name \"entity1name15\";";
        Assert.assertEquals(tc15, result.get(15).toString());

        String tc16 = "$e-16 isa entity1, has entity1-id \"entity1id16\", has entity1-name \"entity1name16\", has entity1-name \"entity1name16-2\";";
        Assert.assertEquals(tc16, result.get(16).toString());

        String tc17 = "$e-17 isa entity1, has entity1-id \"entity1id17\", has entity1-name \"entity1name17\";";
        Assert.assertEquals(tc17, result.get(17).toString());

        String tc18 = "$e-18 isa entity1, has entity1-id \"entity1id18\", has entity1-name \"entity1name18\";";
        Assert.assertEquals(tc18, result.get(18).toString());

        String tc19 = "$e-19 isa entity1, has entity1-id \"entity1id19\", has entity1-name \"entity1name19\";";
        Assert.assertEquals(tc19, result.get(19).toString());

        Assert.assertEquals(20, result.size());
    }



    @Test
    public void graknEntityQueryFromRowWithBoolAndDoubleTest() {
        EntityInsertGenerator testEntityInsertGenerator = new EntityInsertGenerator(migrationConfig.getDataConfig().get("entity2"), genConf.get("processors").get(1));

        ArrayList<String> rows = getData(entity2dp);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));

        ArrayList<Statement> result = testEntityInsertGenerator.graknEntityInsert(rows, header);

        String tc0 = "$e-0 isa entity2, has entity2-id \"entity2id0\", has entity2-bool true, has entity2-double 0.0;";
        Assert.assertEquals(tc0, result.get(0).toString());

        String tc1 = "$e-1 isa entity2, has entity2-id \"entity2id1\", has entity2-bool false, has entity2-double 1.1, has entity2-double 11.11;";
        Assert.assertEquals(tc1, result.get(1).toString());

        String tc2 = "$e-2 isa entity2, has entity2-id \"entity2id2\", has entity2-bool true, has entity2-double 2.2;";
        Assert.assertEquals(tc2, result.get(2).toString());

        String tc3 = "$e-3 isa entity2, has entity2-id \"entity2id3\", has entity2-bool false, has entity2-double -3.3;";
        Assert.assertEquals(tc3, result.get(3).toString());

        String tc4 = "$e-4 isa entity2, has entity2-id \"entity2id4\", has entity2-double 4.0;";
        Assert.assertEquals(tc4, result.get(4).toString());

        String tc5 = "$e-5 isa entity2, has entity2-id \"entity2id5\";";
        Assert.assertEquals(tc5, result.get(5).toString());

        String tc6 = "$e-6 isa entity2, has entity2-id \"entity2id6\";";
        Assert.assertEquals(tc6, result.get(6).toString());

        String tc7 = "$e-7 isa entity2, has entity2-id \"entity2id7\";";
        Assert.assertEquals(tc7, result.get(7).toString());

        String tc8 = "$e-8 isa entity2, has entity2-id \"entity2id8\";";
        Assert.assertEquals(tc8, result.get(8).toString());

        String tc9 = "$e-9 isa entity2, has entity2-id \"entity2id9\";";
        Assert.assertEquals(tc9, result.get(9).toString());

        String tc10 = "$e-10 isa entity2, has entity2-id \"entity2id10\";";
        Assert.assertEquals(tc10, result.get(10).toString());

        Assert.assertEquals(11, result.size());
    }

    @Test
    public void graknEntityQueryFromRowWithLongTest() {

        EntityInsertGenerator testEntityInsertGenerator = new EntityInsertGenerator(migrationConfig.getDataConfig().get("entity3"), genConf.get("processors").get(2));

        ArrayList<String> rows = getData(entity3dp);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));

        ArrayList<Statement> result = testEntityInsertGenerator.graknEntityInsert(rows, header);

        String tc0 = "$e-0 isa entity3, has entity3-id \"entity3id0\", has entity3-int 0;";
        Assert.assertEquals(tc0, result.get(0).toString());

        String tc1 = "$e-1 isa entity3, has entity3-id \"entity3id1\", has entity3-int 1, has entity3-int 11;";
        Assert.assertEquals(tc1, result.get(1).toString());

        String tc2 = "$e-2 isa entity3, has entity3-id \"entity3id2\", has entity3-int 2;";
        Assert.assertEquals(tc2, result.get(2).toString());

        String tc3 = "$e-3 isa entity3, has entity3-id \"entity3id3\", has entity3-int -3;";
        Assert.assertEquals(tc3, result.get(3).toString());

        String tc4 = "$e-4 isa entity3, has entity3-id \"entity3id4\";";
        Assert.assertEquals(tc4, result.get(4).toString());

        String tc5 = "$e-5 isa entity3, has entity3-id \"entity3id5\";";
        Assert.assertEquals(tc5, result.get(5).toString());

        String tc6 = "$e-6 isa entity3, has entity3-id \"entity3id6\";";
        Assert.assertEquals(tc6, result.get(6).toString());

        String tc7 = "$e-7 isa entity3, has entity3-id \"entity3id7\";";
        Assert.assertEquals(tc7, result.get(7).toString());

        String tc8 = "$e-8 isa entity3, has entity3-id \"entity3id8\";";
        Assert.assertEquals(tc8, result.get(8).toString());

        String tc9 = "$e-9 isa entity3, has entity3-id \"entity3id9\";";
        Assert.assertEquals(tc9, result.get(9).toString());

        String tc10 = "$e-10 isa entity3, has entity3-id \"entity3id10\";";
        Assert.assertEquals(tc10, result.get(10).toString());

        Assert.assertEquals(11, result.size());
    }
}