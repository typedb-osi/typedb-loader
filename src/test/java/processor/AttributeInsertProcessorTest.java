//package processor;
//
//import configuration.LoaderLoadConfig;
//import configuration.ConfigEntryProcessor;
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//
//import static test.TestUtil.getData;
//import static util.Util.getAbsPath;
//
//public class AttributeInsertProcessorTest {
//
//    private final String adcp = getAbsPath("src/test/resources/phoneCalls/dataConfig.json");
//    private final String gcp = getAbsPath("src/test/resources/phoneCalls/processorConfig.json");
//    private final String file = getAbsPath("src/test/resources/phoneCalls/is-in-use.csv");
//    private final LoaderLoadConfig loaderLoadConfig = new LoaderLoadConfig("localhost:1729", "null", "null", adcp, gcp);
//    private final HashMap<String, ArrayList<ConfigEntryProcessor>> genConf = loaderLoadConfig.getProcessorConfig();
//
//    @Test
//    public void graknAttributeQueryFromRowTest() {
//
//        AttributeInsertProcessor testAttributeInsertGenerator = new AttributeInsertProcessor(loaderLoadConfig.getDataConfig().get("is-in-use"), genConf.get("processors").get(8), 0);
//        ArrayList<String> rows = getData(file);
//        String header = rows.get(0);
//        rows = new ArrayList<>(rows.subList(1, rows.size()));
//        InsertQueries results = testAttributeInsertGenerator.typeDBInsert(rows, header, 1);
//
//        int idx = 0;
//        String tmp = "$a \"yes\" isa is-in-use";
//        Assert.assertEquals(tmp, results.getDirectInserts().get(idx).toString());
//
//        idx += 1;
//        tmp = "$a \"no\" isa is-in-use";
//        Assert.assertEquals(tmp, results.getDirectInserts().get(idx).toString());
//
//        idx += 1;
//        Assert.assertNull(results.getDirectInserts().get(idx));
//
//        idx += 1;
//        tmp = "$a \"5\" isa is-in-use";
//        Assert.assertEquals(tmp, results.getDirectInserts().get(idx).toString());
//
//        Assert.assertEquals(4, results.getDirectInserts().size());
//        results.getDirectInserts().removeAll(Collections.singleton(null));
//        Assert.assertEquals(3, results.getDirectInserts().size());
//
//        Assert.assertEquals(0, results.getMatchInserts().size());
//        int nullCount = 0;
//        for (int i = 0; i < results.getMatchInserts().size(); i++) {
//            if (results.getMatchInserts().get(i).getMatches() == null) {
//                nullCount++;
//            }
//        }
//        Assert.assertEquals(0, results.getMatchInserts().size() - nullCount);
//    }
//}
