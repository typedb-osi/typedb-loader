package generator;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class GeneratorUtilTest {

    @Test
    public void generatorUtilTestParser() throws IOException {
        String row = "field1,field2,field3";
        String[] test = new String[]{"field1", "field2", "field3"};
        Assert.assertArrayEquals(test, GeneratorUtil.tokenizeCSVStandard(row, ','));

        row = "field1,field2,\"field3,bridged\"";
        test = new String[]{"field1", "field2", "field3,bridged"};
        Assert.assertArrayEquals(test, GeneratorUtil.tokenizeCSVStandard(row, ','));

        row = "field1\tfield2\tfield3";
        test = new String[]{"field1", "field2", "field3"};
        Assert.assertArrayEquals(test, GeneratorUtil.tokenizeCSVStandard(row, '\t'));

        row = "field1\tfield2\t\"field3\tbridged\"";
        test = new String[]{"field1", "field2", "field3\tbridged"};
        Assert.assertArrayEquals(test, GeneratorUtil.tokenizeCSVStandard(row, '\t'));

        row = "";
        test = new String[]{""};
        Assert.assertArrayEquals(test, GeneratorUtil.tokenizeCSVStandard(row, '\t'));

        row = null;
        test = new String[]{""};
        Assert.assertArrayEquals(test, GeneratorUtil.tokenizeCSVStandard(row, '\t'));
    }
}
