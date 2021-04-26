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

public class AppendAttributeGeneratorTest {

    private final String db = "grakn_migrator_test";
    private final String schema = getAbsPath("src/test/resources/phone-calls/schema-updated.gql");
    private final String dc = getAbsPath("src/test/resources/phone-calls/dataConfig.json");
    private final String pc = getAbsPath("src/test/resources/phone-calls/processorConfig.json");
    private final String datafileA = getAbsPath("src/test/resources/phone-calls/append-twitter-nickname.csv");
    private final String datafileB = getAbsPath("src/test/resources/phone-calls/append-fb-preprocessed.csv");
    private final String datafileC = getAbsPath("src/test/resources/phone-calls/append-call-rating.csv");
    private final MigrationConfig migrationConfig = new MigrationConfig("localhost:1729", db, schema, dc, pc);
    private final HashMap<String, ArrayList<ProcessorConfigEntry>> genConf = migrationConfig.getProcessorConfig();

    @Test
    public void graknAttributeQueryFromRowTestA() throws Exception {
        AppendAttributeProcessor testGenerator = new AppendAttributeProcessor(migrationConfig.getDataConfig().get("append-twitter"), genConf.get("processors").get(5), 0);
        ArrayList<String> rows = getData(datafileA);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));
        InsertQueries results = testGenerator.typeDBInsert(rows, header, 1);

        int idx = 0;
        String tmp = "$e isa person, has phone-number \"+7 171 898 0853\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has twitter-username \"@jojo\", has nick-name \"another\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$e isa person, has phone-number \"+263 498 495 0617\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has twitter-username \"@hui\", has twitter-username \"@bui\", has nick-name \"yetanoter\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$e isa person, has phone-number \"+370 351 224 5176\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has twitter-username \"@lalulix\", has nick-name \"one more\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$e isa person, has phone-number \"+81 308 988 7153\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has twitter-username \"@go34\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$e isa person, has phone-number \"+54 398 559 0423\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has twitter-username \"@hadaaa\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        tmp = "$e isa person, has phone-number \"+63 815 962 6097\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has twitter-username \"@kuka\"";
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

        Assert.assertEquals(10, results.getMatchInserts().size());
        int nullCount = 0;
        for (int i = 0; i < results.getMatchInserts().size(); i++) {
            if (results.getMatchInserts().get(i).getMatches() == null) {
                nullCount++;
            }
        }
        Assert.assertEquals(6, results.getMatchInserts().size() - nullCount);
    }

    @Test
    public void graknAttributeQueryFromRowTestB() throws Exception {
        AppendAttributeProcessor testGenerator = new AppendAttributeProcessor(migrationConfig.getDataConfig().get("append-pp-fakebook"), genConf.get("processors").get(7), 0);
        ArrayList<String> rows = getData(datafileB);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));
        InsertQueries results = testGenerator.typeDBInsert(rows, header, 1);

        int idx = 0;
        String tmp = "$e isa person, has phone-number \"+36 318 105 5629\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has fakebook-link \"fakebook.com/personOne\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$e isa person, has phone-number \"+63 808 497 1769\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has fakebook-link \"fakebook.com/person-Two\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$e isa person, has phone-number \"+62 533 266 3426\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has fakebook-link \"fakebook.com/person_three\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$e isa person, has phone-number \"+62 533 266 3426\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has fakebook-link \"insertedWithoutAppliedRegex\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        Assert.assertEquals(0, results.getDirectInserts().size());
        results.getDirectInserts().removeAll(Collections.singleton(null));
        Assert.assertEquals(0, results.getDirectInserts().size());

        Assert.assertEquals(6, results.getMatchInserts().size());
        int nullCount = 0;
        for (int i = 0; i < results.getMatchInserts().size(); i++) {
            if (results.getMatchInserts().get(i).getMatches() == null) {
                nullCount++;
            }
        }
        Assert.assertEquals(4, results.getMatchInserts().size() - nullCount);
    }

    @Test
    public void graknAttributeQueryFromRowTestC() throws Exception {
        AppendAttributeProcessor testGenerator = new AppendAttributeProcessor(migrationConfig.getDataConfig().get("append-call-rating"), genConf.get("processors").get(6), 0);
        ArrayList<String> rows = getData(datafileC);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));
        InsertQueries results = testGenerator.typeDBInsert(rows, header, 1);

        int idx = 0;
        String tmp = "$e isa call, has started-at 2018-09-19T01:00:38;";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has call-rating 5";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$e isa call, has started-at 2018-09-24T03:16:48;";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has call-rating 5";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$e isa call, has started-at 2018-09-26T19:47:20;";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has call-rating 1";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$e isa call, has started-at 2018-09-26T23:47:19;";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has call-rating 2";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        tmp = "$e isa call, has started-at 2018-09-18T04:54:04;";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has call-rating 4";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());

        Assert.assertEquals(0, results.getDirectInserts().size());
        results.getDirectInserts().removeAll(Collections.singleton(null));
        Assert.assertEquals(0, results.getDirectInserts().size());

        Assert.assertEquals(7, results.getMatchInserts().size());
        int nullCount = 0;
        for (int i = 0; i < results.getMatchInserts().size(); i++) {
            if (results.getMatchInserts().get(i).getMatches() == null) {
                nullCount++;
            }
        }
        Assert.assertEquals(5, results.getMatchInserts().size() - nullCount);
    }
}
