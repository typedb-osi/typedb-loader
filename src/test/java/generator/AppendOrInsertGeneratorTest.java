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

public class AppendOrInsertGeneratorTest {

    private final String db = "grakn_migrator_test";
    private final String schema = getAbsPath("src/test/resources/phone-calls/schema-updated.gql");
    private final String dc = getAbsPath("src/test/resources/phone-calls/dataConfig.json");
    private final String pc = getAbsPath("src/test/resources/phone-calls/processorConfig.json");
    private final String datafileA = getAbsPath("src/test/resources/phone-calls/person-append-or-insert.csv");
    private final MigrationConfig migrationConfig = new MigrationConfig("localhost:1729", db, schema, dc, pc);
    private final HashMap<String, ArrayList<ProcessorConfigEntry>> genConf = migrationConfig.getProcessorConfig();

    @Test
    public void graknAttributeQueryFromRowTest() throws Exception {
        AppendOrInsertGenerator testGenerator = new AppendOrInsertGenerator(migrationConfig.getDataConfig().get("person-insert-or-append"), genConf.get("processors").get(10), 0);
        ArrayList<String> rows = getData(datafileA);
        String header = rows.get(0);
        rows = new ArrayList<>(rows.subList(1, rows.size()));
        GeneratorStatement results = testGenerator.graknAppendOrInsertInsert(rows, header, 1);

        int idx = 0;
        String tmp = "$e isa person, has phone-number \"+7 171 898 0853\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has first-name \"Melli\", has last-name \"Winchcum\", has city \"London\", has age 55, has nick-name \"Mel\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());
        tmp = "$e isa person, has first-name \"Melli\", has last-name \"Winchcum\", has phone-number \"+7 171 898 0853\", has city \"London\", has age 55, has nick-name \"Mel\"";
        Assert.assertEquals(tmp, results.getInserts().get(idx).toString());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());
        tmp = "$e isa person, has first-name \"Sakura\", has city \"Fire Village\", has age 13";
        Assert.assertEquals(tmp, results.getInserts().get(idx).toString());

        idx += 1;
        tmp = "$e isa person, has phone-number \"+7 690 597 4443\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has first-name \"Xylina\", has last-name \"D'Alesco\", has city \"Cambridge\", has age 51, has nick-name \"Xyl\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());
        tmp = "$e isa person, has first-name \"Xylina\", has last-name \"D'Alesco\", has phone-number \"+7 690 597 4443\", has city \"Cambridge\", has age 51, has nick-name \"Xyl\"";
        Assert.assertEquals(tmp, results.getInserts().get(idx).toString());

        idx += 1;
        tmp = "$e isa person, has phone-number \"+62 107 666 3334\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has first-name \"Sasuke\", has city \"Fire Village\", has age 13";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());
        tmp = "$e isa person, has first-name \"Sasuke\", has phone-number \"+62 107 666 3334\", has city \"Fire Village\", has age 13";
        Assert.assertEquals(tmp, results.getInserts().get(idx).toString());

        idx += 1;
        tmp = "$e isa person, has phone-number \"+62 107 530 7500\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has first-name \"Elenore\", has last-name \"Stokey\", has city \"Oxford\", has age 35, has nick-name \"Elen\"";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());
        tmp = "$e isa person, has first-name \"Elenore\", has last-name \"Stokey\", has phone-number \"+62 107 530 7500\", has city \"Oxford\", has age 35, has nick-name \"Elen\"";
        Assert.assertEquals(tmp, results.getInserts().get(idx).toString());

        idx += 1;
        tmp = "$e isa person, has phone-number \"+62 107 321 3333\";";
        Assert.assertEquals(tmp, concatMatches(results.getMatchInserts().get(idx).getMatches()));
        tmp = "$e has first-name \"Naruto\", has last-name \"Uzamaki\", has city \"Fire Village\", has age 12";
        Assert.assertEquals(tmp, results.getMatchInserts().get(idx).getInsert().toString());
        tmp = "$e isa person, has first-name \"Naruto\", has last-name \"Uzamaki\", has phone-number \"+62 107 321 3333\", has city \"Fire Village\", has age 12";
        Assert.assertEquals(tmp, results.getInserts().get(idx).toString());

        idx += 1;
        Assert.assertNull(results.getMatchInserts().get(idx).getMatches());
        Assert.assertNull(results.getMatchInserts().get(idx).getInsert());
        Assert.assertNull(results.getInserts().get(idx));

        Assert.assertEquals(7, results.getInserts().size());
        results.getInserts().removeAll(Collections.singleton(null));
        Assert.assertEquals(6, results.getInserts().size());

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
