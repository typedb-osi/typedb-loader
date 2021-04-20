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

public class AppendAttributeGeneratorTest {
    //TODO NEED TO ADD TESTS

    private final String db = "grakn_migrator_test";
    private final String schema = getAbsPath("src/test/resources/phone-calls/schema-updated.gql");
    private final String dc = getAbsPath("src/test/resources/phone-calls/dataConfig.json");
    private final String pc = getAbsPath("src/test/resources/phone-calls/processorConfig.json");
    private final String datafile = getAbsPath("src/test/resources/phone-calls/append-twitter-nickname.csv");
    private final MigrationConfig migrationConfig = new MigrationConfig("localhost:1729", db, schema, dc, pc);
    private final HashMap<String, ArrayList<ProcessorConfigEntry>> genConf = migrationConfig.getProcessorConfig();

    @Test
    public void graknAttributeQueryFromRowTest() throws Exception {
        AppendAttributeGenerator testGenerator = new AppendAttributeGenerator(migrationConfig.getDataConfig().get("append-twitter"), genConf.get("processors").get(5), 0);

        ArrayList<String> rows = getData(datafile);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));

        GeneratorStatement results = testGenerator.graknAppendAttributeInsert(rows, header, 1);

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
    }
}
