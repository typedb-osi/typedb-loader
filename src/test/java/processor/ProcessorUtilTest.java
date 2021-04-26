package processor;

import loader.DataLoader;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class ProcessorUtilTest {

    @Test
    public void processorUtilParserTest() throws IOException {

        // csv
        InputStream csvStream = DataLoader.getInputStream("src/test/resources/generatorutil/test.csv");
        ArrayList<String> csvLines = new ArrayList<>();
        String csvLine;
        assert csvStream != null;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(csvStream))) {
            while ((csvLine = br.readLine()) != null) {
                csvLines.add(csvLine);
            }
        }

        String[] test = new String[]{"field1", "field2", "field3"};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(csvLines.get(0), ','));

        test = new String[]{"field1", "field2", "field3,bridged"};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(csvLines.get(1), ','));

        test = new String[]{"field1", "field2", "field3", "field4 has \"quotes\""};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(csvLines.get(2), ','));

        test = new String[]{"field1", "field2", "field3", "field4 has \\\"quotes\\\""};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(csvLines.get(3), ','));

        test = new String[]{"field1", "field2,field3", "field4 has \"quotes\""};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(csvLines.get(4), ','));

        test = new String[]{"field1", "field2,field3", "field4 has \\\"quotes\\\""};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(csvLines.get(5), ','));

        test = new String[]{"field1", "field2", "field3", "\\\"quotes\\\" at beginning of field"};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(csvLines.get(6), ','));

        test = new String[]{"field1", "field2", "field3", "\\\"quotes\\\" at beginning of field"};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(csvLines.get(7), ','));

        //faulty row because has both "" to escape separator and "" at beginning of field, violating RFC
        Assert.assertEquals(4, ProcessorUtil.tokenizeCSVStandard(csvLines.get(8), ',').length);

        test = new String[]{"field1", "field2,field3", "\\\"quotes\\\" at beginning of field"};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(csvLines.get(9), ','));

        // tsv
        InputStream tsvStream = DataLoader.getInputStream("src/test/resources/generatorutil/test.tsv");
        ArrayList<String> tsvLines = new ArrayList<>();
        String tsvLine;
        assert tsvStream != null;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(tsvStream))) {
            while ((tsvLine = br.readLine()) != null) {
                tsvLines.add(tsvLine);
            }
        }
        test = new String[]{"field1", "field2", "field3"};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(tsvLines.get(0), '\t'));

        test = new String[]{"field1", "field2", "field3\tbridged"};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(tsvLines.get(1), '\t'));

        test = new String[]{"field1", "field2", "field3", "field4 has \"quotes\""};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(tsvLines.get(2), '\t'));

        test = new String[]{"field1", "field2", "field3", "field4 has \\\"quotes\\\""};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(tsvLines.get(3), '\t'));

        test = new String[]{"field1", "field2\tfield3", "field4 has \"quotes\""};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(tsvLines.get(4), '\t'));

        test = new String[]{"field1", "field2\tfield3", "field4 has \\\"quotes\\\""};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(tsvLines.get(5), '\t'));

        test = new String[]{"field1", "field2", "field3", "\\\"quotes\\\" at beginning of field"};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(tsvLines.get(6), '\t'));

        test = new String[]{"field1", "field2", "field3", "\\\"quotes\\\" at beginning of field"};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(tsvLines.get(7), '\t'));

        //faulty row because has both "" to escape separator and "" at beginning of field, violating RFC
        Assert.assertEquals(4, ProcessorUtil.tokenizeCSVStandard(tsvLines.get(8), '\t').length);

        test = new String[]{"field1", "field2\tfield3", "\\\"quotes\\\" at beginning of field"};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(tsvLines.get(9), '\t'));

        // default tests
        String row = "";
        test = new String[]{""};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(row, '\t'));

        row = null;
        test = new String[]{""};
        Assert.assertArrayEquals(test, ProcessorUtil.tokenizeCSVStandard(row, '\t'));

    }

    @Test
    public void processorUtilAddAttributeConstraintTest() {

        Assert.assertEquals("has attributeSchemaType \"attributeValue\"",
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.STRING,
                        "attributeValue",
                        5,
                        null)).toString());

        Assert.assertEquals("has attributeSchemaType 5",
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.LONG,
                        "5",
                        5,
                        null)).toString());

        Assert.assertNull(
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.LONG,
                        "5.2",
                        5,
                        null)));

        Assert.assertNull(
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.LONG,
                        "fail",
                        5,
                        null)));

        Assert.assertEquals("has attributeSchemaType 5.2",
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.DOUBLE,
                        "5.2",
                        5,
                        null)).toString());

        Assert.assertEquals("has attributeSchemaType 5.0",
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.DOUBLE,
                        "5",
                        5,
                        null)).toString());

        Assert.assertNull(
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.DOUBLE,
                        "fail",
                        5,
                        null)));

        Assert.assertEquals("has attributeSchemaType true",
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.BOOLEAN,
                        "true",
                        5,
                        null)).toString());

        Assert.assertEquals("has attributeSchemaType true",
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.BOOLEAN,
                        "TRUE",
                        5,
                        null)).toString());

        Assert.assertEquals("has attributeSchemaType true",
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.BOOLEAN,
                        "TrUe",
                        5,
                        null)).toString());

        Assert.assertEquals("has attributeSchemaType false",
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.BOOLEAN,
                        "false",
                        5,
                        null)).toString());

        Assert.assertEquals("has attributeSchemaType false",
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.BOOLEAN,
                        "FALSE",
                        5,
                        null)).toString());

        Assert.assertEquals("has attributeSchemaType false",
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.BOOLEAN,
                        "FalSe",
                        5,
                        null)).toString());

        Assert.assertNull(
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.BOOLEAN,
                        "fail",
                        5,
                        null)));

        Assert.assertEquals("has attributeSchemaType 2018-09-13T22:10:34",
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.DATETIME,
                        "2018-09-13T22:10:34",
                        5,
                        null)).toString());

        Assert.assertEquals("has attributeSchemaType 2021-04-23T12:44:55",
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.DATETIME,
                        "2021-04-23T12:44:55+03:00",
                        5,
                        null)).toString());

        Assert.assertNull(
                ProcessorUtil.valueToHasConstraint("attributeSchemaType", ProcessorUtil.generateValueConstraint(
                        "attributeSchemaType",
                        AttributeValueType.DATETIME,
                        "fail",
                        5,
                        null)));
    }
}
