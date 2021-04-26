package processor;

import configuration.MigrationConfig;
import configuration.ProcessorConfigEntry;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static test.TestUtil.concatMatches;
import static test.TestUtil.getData;
import static util.Util.getAbsPath;

public class RelationInsertProcessorTest {

    private final String dc = getAbsPath("src/test/resources/genericTests/dataConfig-test.json");
    private final String dcPC = getAbsPath("src/test/resources/phoneCalls/dataConfig.json");
    private final String pc = getAbsPath("src/test/resources/genericTests/processorConfig-test.json");
    private final String pcPC = getAbsPath("src/test/resources/phoneCalls/processorConfig.json");
    private final String dataA = getAbsPath("src/test/resources/genericTests/rel1-test-data.tsv");
    private final String dataB = getAbsPath("src/test/resources/phoneCalls/call.csv");
    private final String dataC = getAbsPath("src/test/resources/phoneCalls/contract.csv");
    private final MigrationConfig migrationConfigA = new MigrationConfig(null, null, null, dc, pc);
    private final HashMap<String, ArrayList<ProcessorConfigEntry>> gcA = migrationConfigA.getProcessorConfig();
    private final MigrationConfig migrationConfigB = new MigrationConfig(null, null, null, dcPC, pcPC);
    private final HashMap<String, ArrayList<ProcessorConfigEntry>> gcB = migrationConfigB.getProcessorConfig();

    @Test
    public void graknRelationQueryFromRowTest() throws Exception {
        RelationInsertProcessor testRelationInsertGenerator = new RelationInsertProcessor(migrationConfigA.getDataConfig().get("rel1"), gcA.get("processors").get(3), 0);
        ArrayList<String> rows = getData(dataA);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));
        InsertQueries results = testRelationInsertGenerator.typeDBInsert(rows, header, 1);

        int idx = 0;
        String tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att0\", has relAt-1 \"explosion0\", has relAt-2 \"opt0\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att1\", has relAt-1 \"explosion1\", has relAt-1 \"explo1\", has relAt-2 \"opt1\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att2\", has relAt-2 \"opt2\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att3\", has relAt-2 \"opt3\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att4\", has relAt-2 \"opt4\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att5\", has relAt-2 \"opt5\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att6\", has relAt-2 \"opt6\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att7\", has relAt-2 \"opt7\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att8\", has relAt-2 \"opt8\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att9\", has relAt-2 \"opt9\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att10\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att11\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att12\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att13\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1) isa rel1, has relAt-1 \"att14\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1) isa rel1, has relAt-1 \"att15\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1) isa rel1, has relAt-1 \"att16\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1) isa rel1, has relAt-1 \"att17\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1) isa rel1, has relAt-1 \"att18\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1) isa rel1, has relAt-1 \"att19\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1) isa rel1, has relAt-1 \"att20\", has relAt-2 \"opt20\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1) isa rel1, has relAt-1 \"att21\", has relAt-1 \"explosion21\", has relAt-2 \"optional21\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att22\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att23\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity2-1 isa entity2, has entity2-id \"entity2id1\";$entity3-2 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-two: $entity2-1, player-optional: $entity3-2) isa rel1, has relAt-1 \"att24\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity1-1 isa entity1, has entity1-id \"entity1id2\";$entity2-2 isa entity2, has entity2-id \"entity2id1\";$entity3-3 isa entity3, has entity3-id \"entity3id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-one: $entity1-1, player-two: $entity2-2, player-optional: $entity3-3) isa rel1, has relAt-1 \"att39\", has relAt-2 \"opt39\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$entity1-0 isa entity1, has entity1-id \"entity1id1\";$entity1-1 isa entity1, has entity1-id \"entity1id2\";$entity2-2 isa entity2, has entity2-id \"entity2id1\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (player-one: $entity1-0, player-one: $entity1-1, player-two: $entity2-2) isa rel1, has relAt-1 \"att40\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());


        Assert.assertEquals(0, results.getDirectInserts().size());
        results.getDirectInserts().removeAll(Collections.singleton(null));
        Assert.assertEquals(0, results.getDirectInserts().size());

        Assert.assertEquals(44, results.getMatchInserts().size());
        int nullCount = 0;
        for (int i = 0; i < results.getMatchInserts().size(); i++) {
            if (results.getMatchInserts().get(i).getMatches() == null) {
                nullCount++;
            }
        }
        Assert.assertEquals(27, results.getMatchInserts().size() - nullCount);

    }

    @Test
    public void graknRelationQueryFromRowCallTest() throws Exception {
        RelationInsertProcessor testRelationInsertGenerator = new RelationInsertProcessor(migrationConfigB.getDataConfig().get("calls"), gcB.get("processors").get(3), 0);
        ArrayList<String> rows = getData(dataB);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));
        InsertQueries results = testRelationInsertGenerator.typeDBInsert(rows, header, 1);

        int idx = 113;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx = 211;
        String tmp = "$person-0 isa person, has phone-number \"+54 398 559 0423\";$person-1 isa person, has phone-number \"+81 746 154 2598\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (caller: $person-0, callee: $person-1) isa call, has started-at 2018-09-18T22:47:52, has duration 5356";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());


        Assert.assertEquals(0, results.getDirectInserts().size());
        results.getDirectInserts().removeAll(Collections.singleton(null));
        Assert.assertEquals(0, results.getDirectInserts().size());

        Assert.assertEquals(218, results.getMatchInserts().size());
        int nullCount = 0;
        for (int i = 0; i < results.getMatchInserts().size(); i++) {
            if (results.getMatchInserts().get(i).getMatches() == null) {
                nullCount++;
            }
        }
        Assert.assertEquals(211, results.getMatchInserts().size() - nullCount);

    }

    @Test
    public void graknRelationQueryFromRowContractTest() throws Exception {
        RelationInsertProcessor testRelationInsertGenerator = new RelationInsertProcessor(migrationConfigB.getDataConfig().get("contract"), gcB.get("processors").get(2), 0);
        ArrayList<String> rows = getData(dataC);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));
        InsertQueries results = testRelationInsertGenerator.typeDBInsert(rows, header, 1);

        int idx = 0;
        String tmp = "$company-0 isa company, has name \"Telecom\";$person-1 isa person, has phone-number \"+7 171 898 0853\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (provider: $company-0, customer: $person-1) isa contract";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$company-0 isa company, has name \"Telecom\";$person-1 isa person, has phone-number \"+370 351 224 5176\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (provider: $company-0, customer: $person-1) isa contract";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$company-0 isa company, has name \"Telecom\";$person-1 isa person, has phone-number \"+81 308 988 7153\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (provider: $company-0, customer: $person-1) isa contract";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$company-0 isa company, has name \"Telecom\";$person-1 isa person, has phone-number \"+54 398 559 0423\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (provider: $company-0, customer: $person-1) isa contract";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$company-0 isa company, has name \"Telecom\";$person-1 isa person, has phone-number \"+7 690 597 4443\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (provider: $company-0, customer: $person-1) isa contract";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$company-0 isa company, has name \"Telecom\";$person-1 isa person, has phone-number \"+263 498 495 0617\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (provider: $company-0, customer: $person-1) isa contract";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$company-0 isa company, has name \"Telecom\";$person-1 isa person, has phone-number \"+63 815 962 6097\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (provider: $company-0, customer: $person-1) isa contract";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$company-0 isa company, has name \"Telecom\";$person-1 isa person, has phone-number \"+81 746 154 2598\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (provider: $company-0, customer: $person-1) isa contract";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        tmp = "$company-0 isa company, has name \"Telecom\";$person-1 isa person, has phone-number \"+261 860 539 4754\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (provider: $company-0, customer: $person-1) isa contract";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        tmp = "$company-0 isa company, has name \"Telecom\";$person-1 isa person, has phone-number \"+62 107 530 7500\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (provider: $company-0, customer: $person-1) isa contract";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$company-0 isa company, has name \"Unity\";$person-1 isa person, has phone-number \"+62 999 888 7777\";$person-2 isa person, has phone-number \"+62 999 888 7778\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (provider: $company-0, customer: $person-1, customer: $person-2) isa contract";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());


        Assert.assertEquals(0, results.getDirectInserts().size());
        results.getDirectInserts().removeAll(Collections.singleton(null));
        Assert.assertEquals(0, results.getDirectInserts().size());

        Assert.assertEquals(14, results.getMatchInserts().size());
        int nullCount = 0;
        for (int i = 0; i < results.getMatchInserts().size(); i++) {
            if (results.getMatchInserts().get(i).getMatches() == null) {
                nullCount++;
            }
        }
        Assert.assertEquals(11, results.getMatchInserts().size() - nullCount);
    }

}