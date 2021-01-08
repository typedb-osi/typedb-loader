package generator;

import configuration.MigrationConfig;
import configuration.ProcessorConfigEntry;
import graql.lang.statement.Statement;
import org.junit.Assert;
import org.junit.Test;

import static util.Util.getAbsPath;

import java.util.ArrayList;
import java.util.HashMap;

import static test.TestUtil.concatMatches;
import static test.TestUtil.getData;

public class RelationInsertGeneratorTest {

    private final String keyspaceName = "grakn_migrator_test";
    private final String asp = getAbsPath("src/test/resources/genericTests/schema-test.gql");
    private final String adcp = getAbsPath("src/test/resources/genericTests/dataConfig-test.json");
    private final String gcp = getAbsPath("src/test/resources/genericTests/processorConfig-test.json");
    private final String rel1dp = getAbsPath("src/test/resources/genericTests/rel1-test-data.tsv");
    private final MigrationConfig migrationConfig = new MigrationConfig("localhost:48555",keyspaceName, asp, adcp, gcp);
    private final HashMap<String, ArrayList<ProcessorConfigEntry>> gc = migrationConfig.getProcessorConfig();

    @Test
    public void graknRelationQueryFromRowTest() throws Exception {

        RelationInsertGenerator testRelationInsertGenerator = new RelationInsertGenerator(migrationConfig.getDataConfig().get("rel1"), gc.get("processors").get(3));

        ArrayList<String> rows = getData(rel1dp);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));

        ArrayList<ArrayList<ArrayList<Statement>>> result = testRelationInsertGenerator.graknRelationInsert(rows, header);

        // test all there
        String tc2m = "$entity1-0-2 isa entity1, has entity1-id \"entity1id1\";$entity2-1-2 isa entity2, has entity2-id \"entity2id1\";$entity3-2-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tc2m, concatMatches(result.get(0).get(2)));
        String tc2i = "$rel-2 (player-one: $entity1-0-2, player-two: $entity2-1-2, player-optional: $entity3-2-2) isa rel1, has relAt-1 \"att2\", has relAt-2 \"opt2\";";
        Assert.assertEquals(tc2i, result.get(1).get(2).get(0).toString());

        // test no optional player & no optional attribute
        String tc15m = "$entity1-0-15 isa entity1, has entity1-id \"entity1id1\";$entity2-1-15 isa entity2, has entity2-id \"entity2id1\";";
        Assert.assertEquals(tc15m, concatMatches(result.get(0).get(15)));
        String tc15i = "$rel-15 (player-one: $entity1-0-15, player-two: $entity2-1-15) isa rel1, has relAt-1 \"att15\";";
        Assert.assertEquals(tc15i, result.get(1).get(15).get(0).toString());

        // test attribute explosion
        String tc0m = "$entity1-0-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1-0 isa entity2, has entity2-id \"entity2id1\";$entity3-2-0 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tc0m, concatMatches(result.get(0).get(0)));
        String tc0i = "$rel-0 (player-one: $entity1-0-0, player-two: $entity2-1-0, player-optional: $entity3-2-0) isa rel1, has relAt-1 \"att0\", has relAt-1 \"explosion0\", has relAt-2 \"opt0\";";
        Assert.assertEquals(tc0i, result.get(1).get(0).get(0).toString());

        // test empty explosion
        String tc10m = "$entity1-0-9 isa entity1, has entity1-id \"entity1id1\";$entity2-1-9 isa entity2, has entity2-id \"entity2id1\";$entity3-2-9 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tc10m, concatMatches(result.get(0).get(9)));
        String tc10i = "$rel-9 (player-one: $entity1-0-9, player-two: $entity2-1-9, player-optional: $entity3-2-9) isa rel1, has relAt-1 \"att9\", has relAt-2 \"opt9\";";
        Assert.assertEquals(tc10i, result.get(1).get(9).get(0).toString());

        // test exploded player complete
        String tc25m = "$entity1-0-25 isa entity1, has entity1-id \"entity1id1\";$entity1-1-25 isa entity1, has entity1-id \"entity1id2\";$entity2-2-25 isa entity2, has entity2-id \"entity2id1\";$entity3-3-25 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tc25m, concatMatches(result.get(0).get(25)));
        String tc25i = "$rel-25 (player-one: $entity1-0-25, player-one: $entity1-1-25, player-two: $entity2-2-25, player-optional: $entity3-3-25) isa rel1, has relAt-1 \"att39\", has relAt-2 \"opt39\";";
        Assert.assertEquals(tc25i, result.get(1).get(25).get(0).toString());

        // test exploded player without optional player and optional attribute
        String tc26m = "$entity1-0-26 isa entity1, has entity1-id \"entity1id1\";$entity1-1-26 isa entity1, has entity1-id \"entity1id2\";$entity2-2-26 isa entity2, has entity2-id \"entity2id1\";";
        Assert.assertEquals(tc26m, concatMatches(result.get(0).get(26)));
        String tc26i = "$rel-26 (player-one: $entity1-0-26, player-one: $entity1-1-26, player-two: $entity2-2-26) isa rel1, has relAt-1 \"att40\";";
        Assert.assertEquals(tc26i, result.get(1).get(26).get(0).toString());

        Assert.assertEquals(2, result.size());

        // number of match statements = number of valid statements that would be inserted
        Assert.assertEquals(27, result.get(0).size());
        Assert.assertEquals(27, result.get(1).size());

    }

}