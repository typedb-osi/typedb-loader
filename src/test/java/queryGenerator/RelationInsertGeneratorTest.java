package queryGenerator;

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
    private final HashMap<String, ArrayList<ProcessorConfigEntry>> gc = migrationConfig.getGeneratorConfig();

    @Test
    public void graknRelationQueryFromRowTest() throws Exception {

        RelationInsertGenerator testRelationInsertGenerator = new RelationInsertGenerator(migrationConfig.getDataConfig().get("rel1"), gc.get("processors").get(3));

        ArrayList<String> rows = getData(rel1dp);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));

        ArrayList<ArrayList<ArrayList<Statement>>> result = testRelationInsertGenerator.graknRelationInsert(rows, header);

        String tc0m = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-0 isa entity2, has entity2-id \"entity2id1\";$entity3-0 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tc0m, concatMatches(result.get(0).get(0)));
        String tc0i = "$rel-0 (player-one: $entity1-0, player-two: $entity2-0, player-optional: $entity3-0) isa rel1, has relAt-1 \"att0\", has relAt-1 \"explosion0\", has relAt-2 \"opt0\";";
        Assert.assertEquals(tc0i, result.get(1).get(0).get(0).toString());

        String tc10m = "$entity1-9 isa entity1, has entity1-id \"entity1id1\";$entity2-9 isa entity2, has entity2-id \"entity2id1\";$entity3-9 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tc10m, concatMatches(result.get(0).get(9)));
        String tc10i = "$rel-9 (player-one: $entity1-9, player-two: $entity2-9, player-optional: $entity3-9) isa rel1, has relAt-1 \"att9\", has relAt-2 \"opt9\";";
        Assert.assertEquals(tc10i, result.get(1).get(9).get(0).toString());

        Assert.assertEquals(2, result.size());
        Assert.assertEquals(25, result.get(0).size());
    }

}