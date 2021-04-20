package generator;

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

public class NestedRelationInsertGeneratorTest {

    private final String dc = getAbsPath("src/test/resources/phone-calls/dataConfig.json");
    private final String pc = getAbsPath("src/test/resources/phone-calls/processorConfig.json");
    private final String dataA = getAbsPath("src/test/resources/phone-calls/communication-channel.csv");
    private final String dataB = getAbsPath("src/test/resources/phone-calls/communication-channel-pm.csv");
    private final MigrationConfig migrationConfig = new MigrationConfig(null, null, null, dc, pc);
    private final HashMap<String, ArrayList<ProcessorConfigEntry>> gc = migrationConfig.getProcessorConfig();

    @Test
    public void graknNestedRelationQueryFromRowTest() throws Exception {
        RelationInsertGenerator testRelationInsertGenerator = new NestedRelationInsertGenerator(migrationConfig.getDataConfig().get("communication-channel"), gc.get("processors").get(4), 0);
        ArrayList<String> rows = getData(dataA);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));
        GeneratorStatement results = testRelationInsertGenerator.graknRelationInsert(rows, header, 1);

        int idx = 0;
        String tmp = "$person-0 isa person, has phone-number \"+54 398 559 0423\";$person-1 isa person, has phone-number \"+48 195 624 2025\";$call-2 isa call, has started-at 2018-09-16T22:24:19;";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (peer: $person-0, peer: $person-1, past-call: $call-2) isa communication-channel";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$person-0 isa person, has phone-number \"+263 498 495 0617\";$person-1 isa person, has phone-number \"+33 614 339 0298\";$call-2 isa call, has started-at 2018-09-11T22:10:34;$call-3 isa call, has started-at 2018-09-12T22:10:34;$call-4 isa call, has started-at 2018-09-13T22:10:34;$call-5 isa call, has started-at 2018-09-14T22:10:34;$call-6 isa call, has started-at 2018-09-15T22:10:34;$call-7 isa call, has started-at 2018-09-16T22:10:34;";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (peer: $person-0, peer: $person-1, past-call: $call-2, past-call: $call-3, past-call: $call-4, past-call: $call-5, past-call: $call-6, past-call: $call-7) isa communication-channel";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$person-0 isa person, has phone-number \"+370 351 224 5176\";$person-1 isa person, has phone-number \"+62 533 266 3426\";$call-2 isa call, has started-at 2018-09-15T12:12:59;";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (peer: $person-0, peer: $person-1, past-call: $call-2) isa communication-channel";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$person-0 isa person, has phone-number \"+62 533 266 3426\";$call-1 isa call, has started-at 2018-09-15T12:12:59;";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (peer: $person-0, past-call: $call-1) isa communication-channel";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$person-0 isa person, has phone-number \"+370 351 224 5176\";$call-1 isa call, has started-at 2018-09-15T12:12:59;";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (peer: $person-0, past-call: $call-1) isa communication-channel";
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

        Assert.assertEquals(0, results.getInserts().size());
        results.getInserts().removeAll(Collections.singleton(null));
        Assert.assertEquals(0, results.getInserts().size());

        Assert.assertEquals(11, results.getMatchInserts().size());
        int nullCount = 0;
        for (int i = 0; i < results.getMatchInserts().size(); i++) {
            if (results.getMatchInserts().get(i).getMatches() == null) {
                nullCount++;
            }
        }
        Assert.assertEquals(5, results.getMatchInserts().size() - nullCount);
    }

    @Test
    public void graknNestedRelationQueryFromRowPMTest() throws Exception {
        RelationInsertGenerator testRelationInsertGenerator = new NestedRelationInsertGenerator(migrationConfig.getDataConfig().get("communication-channel-pm"), gc.get("processors").get(4), 0);
        ArrayList<String> rows = getData(dataB);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));
        GeneratorStatement results = testRelationInsertGenerator.graknRelationInsert(rows, header, 1);

        int idx = 0;
//        String tmp = "$relplayer-player-0 isa person, has phone-number \"+81 308 988 7153\";$relplayer-player-1 isa person, has phone-number \"+351 515 605 7915\";$call-2 (caller: $relplayer-player-0, callee: $relplayer-player-1) isa call;";
        String tmp = "$person-0 isa person, has phone-number \"+81 308 988 7153\";$person-1 isa person, has phone-number \"+351 515 605 7915\";$relplayer-player-0 isa person, has phone-number \"+81 308 988 7153\";$relplayer-player-1 isa person, has phone-number \"+351 515 605 7915\";$call-2 (caller: $relplayer-player-0, callee: $relplayer-player-1) isa call;";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (peer: $person-0, peer: $person-1, past-call: $call-2) isa communication-channel";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$person-0 isa person, has phone-number \"+7 171 898 0853\";$person-1 isa person, has phone-number \"+57 629 420 5680\";$relplayer-player-0 isa person, has phone-number \"+7 171 898 0853\";$relplayer-player-1 isa person, has phone-number \"+57 629 420 5680\";$call-2 (caller: $relplayer-player-0, callee: $relplayer-player-1) isa call;";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$rel (peer: $person-0, peer: $person-1, past-call: $call-2) isa communication-channel";
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

        Assert.assertEquals(0, results.getInserts().size());
        results.getInserts().removeAll(Collections.singleton(null));
        Assert.assertEquals(0, results.getInserts().size());

        Assert.assertEquals(7, results.getMatchInserts().size());
        int nullCount = 0;
        for (int i = 0; i < results.getMatchInserts().size(); i++) {
            if (results.getMatchInserts().get(i).getMatches() == null) {
                nullCount++;
            }
        }
        Assert.assertEquals(2, results.getMatchInserts().size() - nullCount);
    }

}