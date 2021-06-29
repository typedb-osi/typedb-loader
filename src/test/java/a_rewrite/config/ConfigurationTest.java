package a_rewrite.config;

import a_rewrite.type.AttributeValueType;
import a_rewrite.util.Util;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.ObjectInputFilter;

public class ConfigurationTest {

    @Test
    public void dcTest() {
        Configuration dc = Util.initializeDataConfig(new File("src/test/resources/1.0.0/synthetic/dc.json").getAbsolutePath());
        //general
        Assert.assertNotNull(dc);

        //default config
        Assert.assertEquals(',', dc.getDefaultConfig().getSeparator().charValue());

        //attributes
        Assert.assertEquals("src/test/resources/1.0.0/synthetic/names.csv", dc.getAttributes().get("names").getDataPaths()[0]);
        Assert.assertEquals("name", dc.getAttributes().get("names").getColumn());
        Assert.assertEquals("name", dc.getAttributes().get("names").getConceptType());
        Assert.assertNull(dc.getAttributes().get("names").getConceptValueType());
        Assert.assertNull(dc.getAttributes().get("names").getListSeparator());
        Assert.assertNull(dc.getAttributes().get("names").getPreprocessorConfig());
        Assert.assertEquals("###", dc.getAttributes().get("names-list-separated").getListSeparator());
    }

    @Test
    public void dcPhoneCallsTest() {
        Configuration dc = Util.initializeDataConfig(new File("src/test/resources/1.0.0/phoneCalls/dc.json").getAbsolutePath());
        //general
        Assert.assertNotNull(dc);

        //default config
        Assert.assertEquals(',', dc.getDefaultConfig().getSeparator().charValue());

        //attributes
        String att = "is-in-use";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/is-in-use.csv", dc.getAttributes().get(att).getDataPaths()[0]);
        Assert.assertEquals("values", dc.getAttributes().get(att).getColumn());
        Assert.assertEquals("is-in-use", dc.getAttributes().get(att).getConceptType());
        Assert.assertEquals('\t', dc.getAttributes().get(att).getSeparator().charValue());
        Assert.assertNull(dc.getAttributes().get(att).getConceptValueType());
        Assert.assertNull(dc.getAttributes().get(att).getListSeparator());
        Assert.assertNull(dc.getAttributes().get(att).getPreprocessorConfig());
        Assert.assertEquals(50, dc.getAttributes().get(att).getRowsPerCommit().intValue());

        //entities
        String entity = "person";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/person.csv", dc.getEntities().get(entity).getDataPaths()[0]);
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/person_nextfile.csv", dc.getEntities().get(entity).getDataPaths()[1]);
        Assert.assertEquals(',', dc.getEntities().get(entity).getSeparator().charValue());
        Assert.assertEquals("person", dc.getEntities().get(entity).getConceptType());
        Assert.assertEquals(50, dc.getEntities().get(entity).getRowsPerCommit().intValue());
        Configuration.HasAttribute[] attributes = dc.getEntities().get(entity).getAttributes();
        Assert.assertEquals("first-name", attributes[0].getConceptType());
        Assert.assertNull(attributes[0].getConceptValueType());
        Assert.assertEquals("first_name", attributes[0].getColumn());
        Assert.assertFalse(attributes[0].getRequireNonEmpty());
        Assert.assertNull(attributes[0].getListSeparator());
        Assert.assertNull(attributes[0].getPreprocessorConfig());
        Assert.assertTrue(attributes[2].getRequireNonEmpty());
        Assert.assertEquals(";", attributes[5].getListSeparator());

        //entity-relations
        String relation = "contract";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/contract.csv", dc.getRelations().get(relation).getDataPaths()[0]);
        Assert.assertEquals(',', dc.getRelations().get(relation).getSeparator().charValue());
        Assert.assertEquals("contract", dc.getRelations().get(relation).getConceptType());
        Assert.assertEquals(100, dc.getRelations().get(relation).getRowsPerCommit().intValue());
        Assert.assertNull(dc.getRelations().get(relation).getAttributes());

        Configuration.Player[] players = dc.getRelations().get(relation).getPlayers();
        Configuration.Player player = players[0];
        Assert.assertEquals("provider", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        Configuration.Getter getter = player.getGetter()[0];
        Assert.assertEquals("company", getter.getConceptType());
        Assert.assertEquals("entity", getter.getHandler().toString());
        Assert.assertNull(getter.getColumn());
        Assert.assertNull(getter.getListSeparator());
        getter = player.getGetter()[1];
        Assert.assertEquals("name", getter.getConceptType());
        Assert.assertEquals("ownership", getter.getHandler().toString());
        Assert.assertEquals("company_name", getter.getColumn());
        Assert.assertNull(getter.getListSeparator());

        player = players[1];
        Assert.assertEquals("customer", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        getter = player.getGetter()[0];
        Assert.assertEquals("person", getter.getConceptType());
        Assert.assertEquals("entity", getter.getHandler().toString());
        Assert.assertNull(getter.getColumn());
        Assert.assertNull(getter.getListSeparator());
        getter = player.getGetter()[1];
        Assert.assertEquals("phone-number", getter.getConceptType());
        Assert.assertEquals("ownership", getter.getHandler().toString());
        Assert.assertEquals("person_id", getter.getColumn());
        Assert.assertEquals("###", getter.getListSeparator());

        relation = "call";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/call.csv", dc.getRelations().get(relation).getDataPaths()[0]);
        Assert.assertEquals(',', dc.getRelations().get(relation).getSeparator().charValue());
        Assert.assertEquals(100, dc.getRelations().get(relation).getRowsPerCommit().intValue());
        Assert.assertEquals("call", dc.getRelations().get(relation).getConceptType());

        players = dc.getRelations().get(relation).getPlayers();
        player = players[0];
        Assert.assertEquals("caller", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        getter = player.getGetter()[0];
        Assert.assertEquals("person", getter.getConceptType());
        Assert.assertEquals("entity", getter.getHandler().toString());
        Assert.assertNull(getter.getColumn());
        Assert.assertNull(getter.getListSeparator());
        getter = player.getGetter()[1];
        Assert.assertEquals("phone-number", getter.getConceptType());
        Assert.assertEquals("ownership", getter.getHandler().toString());
        Assert.assertEquals("caller_id", getter.getColumn());
        Assert.assertNull(getter.getListSeparator());

        player = players[1];
        Assert.assertEquals("callee", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        getter = player.getGetter()[0];
        Assert.assertEquals("person", getter.getConceptType());
        Assert.assertEquals("entity", getter.getHandler().toString());
        Assert.assertNull(getter.getColumn());
        Assert.assertNull(getter.getListSeparator());
        getter = player.getGetter()[1];
        Assert.assertEquals("phone-number", getter.getConceptType());
        Assert.assertEquals("ownership", getter.getHandler().toString());
        Assert.assertEquals("callee_id", getter.getColumn());
        Assert.assertNull(getter.getListSeparator());

        attributes = dc.getRelations().get(relation).getAttributes();
        Configuration.HasAttribute attribute = attributes[0];
        Assert.assertEquals("started-at", attribute.getConceptType());
        Assert.assertEquals("started_at", attribute.getColumn());
        Assert.assertTrue(attribute.getRequireNonEmpty());
        Assert.assertNull(attribute.getPreprocessorConfig());
        Assert.assertNull(attribute.getListSeparator());

        attribute = attributes[1];
        Assert.assertEquals("duration", attribute.getConceptType());
        Assert.assertEquals("duration", attribute.getColumn());
        Assert.assertTrue(attribute.getRequireNonEmpty());
        Assert.assertNull(attribute.getPreprocessorConfig());
        Assert.assertNull(attribute.getListSeparator());
    }

}
