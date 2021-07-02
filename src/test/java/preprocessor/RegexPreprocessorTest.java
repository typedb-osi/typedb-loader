package preprocessor;

import preprocessor.RegexPreprocessor;
import org.junit.Assert;
import org.junit.Test;

public class RegexPreprocessorTest {

    @Test
    public void regexPreprocessorTest() {
        RegexPreprocessor rpp = new RegexPreprocessor("^.*(fakebook\\.com.*)/$", "$1");
        String test = "https://www.fakebook.com/personOne/";
        String res = "fakebook.com/personOne";
        Assert.assertEquals(res, rpp.applyProcessor(test));

        test = "www.fakebook.com/personOne/";
        res = "fakebook.com/personOne";
        Assert.assertEquals(res, rpp.applyProcessor(test));

        test = "fakebook.com/personOne/";
        res = "fakebook.com/personOne";
        Assert.assertEquals(res, rpp.applyProcessor(test));

        test = "personOne/";
        res = "personOne/";
        Assert.assertEquals(res, rpp.applyProcessor(test));

        test = "insertedWithoutAppliedRegex";
        res = "insertedWithoutAppliedRegex";
        Assert.assertEquals(res, rpp.applyProcessor(test));

        test = "";
        res = "";
        Assert.assertEquals(res, rpp.applyProcessor(test));
    }

}