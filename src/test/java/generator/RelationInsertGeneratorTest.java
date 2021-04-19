package generator;

import configuration.MigrationConfig;
import configuration.ProcessorConfigEntry;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static test.TestUtil.concatMatches;
import static test.TestUtil.getData;
import static util.Util.getAbsPath;

public class RelationInsertGeneratorTest {

    private final String adcp = getAbsPath("src/test/resources/genericTests/dataConfig-test.json");
    private final String gcp = getAbsPath("src/test/resources/genericTests/processorConfig-test.json");
    private final String rel1dp = getAbsPath("src/test/resources/genericTests/rel1-test-data.tsv");
    private final MigrationConfig migrationConfig = new MigrationConfig(null, null, null, adcp, gcp);
    private final HashMap<String, ArrayList<ProcessorConfigEntry>> gc = migrationConfig.getProcessorConfig();

    @Test
    public void graknRelationQueryFromRowTest() throws Exception {

        RelationInsertGenerator testRelationInsertGenerator = new RelationInsertGenerator(migrationConfig.getDataConfig().get("rel1"), gc.get("processors").get(3), 0);

        ArrayList<String> rows = getData(rel1dp);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));

        GeneratorStatements results = testRelationInsertGenerator.graknRelationInsert(rows, header, 1);

        // test all there
        String tc2m = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tc2m, concatMatches(results.getMatchInserts().get(2).getMatches()));
        String tc2i = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att2\", has relAt-2 \"opt2\"";
        Assert.assertEquals(tc2i, results.getMatchInserts().get(2).getInsert().toString());

        // test no optional player & no optional attribute
        String tc15m = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";";
        Assert.assertEquals(tc15m, concatMatches(results.getMatchInserts().get(15).getMatches()));
        String tc15i = "$rel (player-one: $entity1-0, player-two: $entity2-1) isa rel1, has relAt-1 \"att15\"";
        Assert.assertEquals(tc15i, results.getMatchInserts().get(15).getInsert().toString());

        // test attribute explosion
        String tc0m = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tc0m, concatMatches(results.getMatchInserts().get(0).getMatches()));
        String tc0i = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att0\", has relAt-1 \"explosion0\", has relAt-2 \"opt0\"";
        Assert.assertEquals(tc0i, results.getMatchInserts().get(0).getInsert().toString());

        // test empty explosion
        String tc10m = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tc10m, concatMatches(results.getMatchInserts().get(9).getMatches()));
        String tc10i = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att9\", has relAt-2 \"opt9\"";
        Assert.assertEquals(tc10i, results.getMatchInserts().get(9).getInsert().toString());

        // test exploded player complete
        String tc25m = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity1-1 isa entity1, has entity1-id \"entity1id2\";$entity2-2 isa entity2, has entity2-id \"entity2id1\";$entity3-3 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tc25m, concatMatches(results.getMatchInserts().get(25).getMatches()));
        String tc25i = "$rel (player-one: $entity1-0, player-one: $entity1-1, player-two: $entity2-2, player-optional: $entity3-3) isa rel1, has relAt-1 \"att39\", has relAt-2 \"opt39\"";
        Assert.assertEquals(tc25i, results.getMatchInserts().get(25).getInsert().toString());

        // test exploded player without optional player and optional attribute
        String tc26m = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity1-1 isa entity1, has entity1-id \"entity1id2\";$entity2-2 isa entity2, has entity2-id \"entity2id1\";";
        Assert.assertEquals(tc26m, concatMatches(results.getMatchInserts().get(26).getMatches()));
        String tc26i = "$rel (player-one: $entity1-0, player-one: $entity1-1, player-two: $entity2-2) isa rel1, has relAt-1 \"att40\"";
        Assert.assertEquals(tc26i, results.getMatchInserts().get(26).getInsert().toString());

        // number of match ThingVariables = number of valid ThingVariables that would be inserted
        Assert.assertEquals(27, results.getMatchInserts().size());

    }

}