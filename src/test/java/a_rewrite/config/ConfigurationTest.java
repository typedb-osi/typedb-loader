package a_rewrite.config;

import a_rewrite.util.Util;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class ConfigurationTest {

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
        Assert.assertEquals("values", dc.getAttributes().get(att).getAttribute().getColumn());
        Assert.assertEquals("is-in-use", dc.getAttributes().get(att).getAttribute().getConceptType());
        Assert.assertEquals(',', dc.getAttributes().get(att).getConfig().getSeparator().charValue());
        Assert.assertNull(dc.getAttributes().get(att).getAttribute().getConceptValueType());
        Assert.assertNull(dc.getAttributes().get(att).getAttribute().getListSeparator());
        Assert.assertNull(dc.getAttributes().get(att).getAttribute().getPreprocessorConfig());
        Assert.assertEquals(50, dc.getAttributes().get(att).getConfig().getRowsPerCommit().intValue());

        //entities
        String entity = "person";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/person.csv", dc.getEntities().get(entity).getDataPaths()[0]);
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/person_nextfile.csv", dc.getEntities().get(entity).getDataPaths()[1]);
        Assert.assertEquals(',', dc.getEntities().get(entity).getConfig().getSeparator().charValue());
        Assert.assertEquals("person", dc.getEntities().get(entity).getConceptType());
        Assert.assertEquals(50, dc.getEntities().get(entity).getConfig().getRowsPerCommit().intValue());
        Configuration.ConstrainingAttribute[] attributes = dc.getEntities().get(entity).getAttributes();
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
        Assert.assertEquals(',', dc.getRelations().get(relation).getConfig().getSeparator().charValue());
        Assert.assertEquals("contract", dc.getRelations().get(relation).getConceptType());
        Assert.assertEquals(50, dc.getRelations().get(relation).getConfig().getRowsPerCommit().intValue());
        Assert.assertNull(dc.getRelations().get(relation).getAttributes());

        Configuration.Player[] players = dc.getRelations().get(relation).getPlayers();
        Configuration.Player player = players[0];
        Assert.assertEquals("provider", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        Configuration.RoleGetter roleGetter = player.getRoleGetter();
        Assert.assertEquals("company", roleGetter.getConceptType());
        Assert.assertEquals("entity", roleGetter.getHandler().toString());
        Assert.assertNull(roleGetter.getColumn());
        Assert.assertNull(roleGetter.getListSeparator());
        Configuration.ThingGetter thingGetter = player.getRoleGetter().getThingGetters()[0];
        Assert.assertEquals("name", thingGetter.getConceptType());
        Assert.assertEquals("ownership", thingGetter.getHandler().toString());
        Assert.assertEquals("company_name", thingGetter.getColumn());
        Assert.assertNull(thingGetter.getListSeparator());

        player = players[1];
        Assert.assertEquals("customer", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        roleGetter = player.getRoleGetter();
        Assert.assertEquals("person", roleGetter.getConceptType());
        Assert.assertEquals("entity", roleGetter.getHandler().toString());
        Assert.assertNull(roleGetter.getColumn());
        Assert.assertNull(roleGetter.getListSeparator());
        thingGetter = player.getRoleGetter().getThingGetters()[0];
        Assert.assertEquals("phone-number", thingGetter.getConceptType());
        Assert.assertEquals("ownership", thingGetter.getHandler().toString());
        Assert.assertEquals("person_id", thingGetter.getColumn());
        Assert.assertEquals("###", thingGetter.getListSeparator());

        relation = "call";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/call.csv", dc.getRelations().get(relation).getDataPaths()[0]);
        Assert.assertEquals(',', dc.getRelations().get(relation).getConfig().getSeparator().charValue());
        Assert.assertEquals(50, dc.getRelations().get(relation).getConfig().getRowsPerCommit().intValue());
        Assert.assertEquals("call", dc.getRelations().get(relation).getConceptType());

        players = dc.getRelations().get(relation).getPlayers();
        player = players[0];
        Assert.assertEquals("caller", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        roleGetter = player.getRoleGetter();
        Assert.assertEquals("person", roleGetter.getConceptType());
        Assert.assertEquals("entity", roleGetter.getHandler().toString());
        Assert.assertNull(roleGetter.getColumn());
        Assert.assertNull(roleGetter.getListSeparator());
        thingGetter = player.getRoleGetter().getThingGetters()[0];
        Assert.assertEquals("phone-number", thingGetter.getConceptType());
        Assert.assertEquals("ownership", thingGetter.getHandler().toString());
        Assert.assertEquals("caller_id", thingGetter.getColumn());
        Assert.assertNull(thingGetter.getListSeparator());

        player = players[1];
        Assert.assertEquals("callee", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        roleGetter = player.getRoleGetter();
        Assert.assertEquals("person", roleGetter.getConceptType());
        Assert.assertEquals("entity", roleGetter.getHandler().toString());
        Assert.assertNull(roleGetter.getColumn());
        Assert.assertNull(roleGetter.getListSeparator());
        thingGetter = player.getRoleGetter().getThingGetters()[0];
        Assert.assertEquals("phone-number", thingGetter.getConceptType());
        Assert.assertEquals("ownership", thingGetter.getHandler().toString());
        Assert.assertEquals("callee_id", thingGetter.getColumn());
        Assert.assertNull(thingGetter.getListSeparator());

        attributes = dc.getRelations().get(relation).getAttributes();
        Configuration.ConstrainingAttribute attribute = attributes[0];
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

        //Nested-Relation by Attribute:
        relation = "communication-channel";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/communication-channel.csv", dc.getRelations().get(relation).getDataPaths()[0]);
        Assert.assertEquals(',', dc.getRelations().get(relation).getConfig().getSeparator().charValue());
        Assert.assertEquals(50, dc.getRelations().get(relation).getConfig().getRowsPerCommit().intValue());
        Assert.assertEquals("communication-channel", dc.getRelations().get(relation).getConceptType());

        players = dc.getRelations().get(relation).getPlayers();
        player = players[0];
        Assert.assertEquals("peer", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        roleGetter = player.getRoleGetter();
        Assert.assertEquals("person", roleGetter.getConceptType());
        Assert.assertEquals("entity", roleGetter.getHandler().toString());
        Assert.assertNull(roleGetter.getColumn());
        Assert.assertNull(roleGetter.getListSeparator());
        thingGetter = roleGetter.getThingGetters()[0];
        Assert.assertEquals("phone-number", thingGetter.getConceptType());
        Assert.assertEquals("ownership", thingGetter.getHandler().toString());
        Assert.assertEquals("peer_1", thingGetter.getColumn());
        Assert.assertNull(thingGetter.getListSeparator());

        player = players[1];
        Assert.assertEquals("peer", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        roleGetter = player.getRoleGetter();
        Assert.assertEquals("person", roleGetter.getConceptType());
        Assert.assertEquals("entity", roleGetter.getHandler().toString());
        Assert.assertNull(roleGetter.getColumn());
        Assert.assertNull(roleGetter.getListSeparator());
        thingGetter = roleGetter.getThingGetters()[0];
        Assert.assertEquals("phone-number", thingGetter.getConceptType());
        Assert.assertEquals("ownership", thingGetter.getHandler().toString());
        Assert.assertEquals("peer_2", thingGetter.getColumn());
        Assert.assertNull(thingGetter.getListSeparator());

        player = players[2];
        Assert.assertEquals("past-call", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        roleGetter = player.getRoleGetter();
        Assert.assertEquals("call", roleGetter.getConceptType());
        Assert.assertEquals("relation", roleGetter.getHandler().toString());
        Assert.assertNull(roleGetter.getColumn());
        Assert.assertNull(roleGetter.getListSeparator());
        thingGetter = roleGetter.getThingGetters()[0];
        Assert.assertEquals("started-at", thingGetter.getConceptType());
        Assert.assertEquals("ownership", thingGetter.getHandler().toString());
        Assert.assertEquals("call_started_at", thingGetter.getColumn());
        Assert.assertEquals("###", thingGetter.getListSeparator());

        //Nested-Relation by Players:
        relation = "communication-channel-pm";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/communication-channel-pm.csv", dc.getRelations().get(relation).getDataPaths()[0]);
        Assert.assertEquals(',', dc.getRelations().get(relation).getConfig().getSeparator().charValue());
        Assert.assertEquals(50, dc.getRelations().get(relation).getConfig().getRowsPerCommit().intValue());
        Assert.assertEquals("communication-channel", dc.getRelations().get(relation).getConceptType());

        players = dc.getRelations().get(relation).getPlayers();
        player = players[0];
        Assert.assertEquals("peer", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        roleGetter = player.getRoleGetter();
        Assert.assertEquals("person", roleGetter.getConceptType());
        Assert.assertEquals("entity", roleGetter.getHandler().toString());
        Assert.assertNull(roleGetter.getColumn());
        Assert.assertNull(roleGetter.getListSeparator());
        thingGetter = roleGetter.getThingGetters()[0];
        Assert.assertEquals("phone-number", thingGetter.getConceptType());
        Assert.assertEquals("ownership", thingGetter.getHandler().toString());
        Assert.assertEquals("peer_1", thingGetter.getColumn());
        Assert.assertNull(thingGetter.getListSeparator());

        player = players[1];
        Assert.assertEquals("peer", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        roleGetter = player.getRoleGetter();
        Assert.assertEquals("person", roleGetter.getConceptType());
        Assert.assertEquals("entity", roleGetter.getHandler().toString());
        Assert.assertNull(roleGetter.getColumn());
        Assert.assertNull(roleGetter.getListSeparator());
        thingGetter = roleGetter.getThingGetters()[0];
        Assert.assertEquals("phone-number", thingGetter.getConceptType());
        Assert.assertEquals("ownership", thingGetter.getHandler().toString());
        Assert.assertEquals("peer_2", thingGetter.getColumn());
        Assert.assertNull(thingGetter.getListSeparator());

        player = players[2];
        Assert.assertEquals("past-call", player.getRoleType());
        Assert.assertTrue(player.getRequireNonEmpty());
        roleGetter = player.getRoleGetter();
        Assert.assertEquals("call", roleGetter.getConceptType());
        Assert.assertEquals("relation", roleGetter.getHandler().toString());
        Assert.assertNull(roleGetter.getColumn());
        Assert.assertNull(roleGetter.getListSeparator());
        //first person to use for relation identification
        thingGetter = roleGetter.getThingGetters()[0];
        Assert.assertEquals("person", thingGetter.getConceptType());
        Assert.assertEquals("entity", thingGetter.getHandler().toString());
        Configuration.ThingGetter thingThingGetter = thingGetter.getThingGetters()[0];
        Assert.assertEquals("phone-number", thingThingGetter.getConceptType());
        Assert.assertEquals("ownership", thingThingGetter.getHandler().toString());
        Assert.assertEquals("peer_1", thingThingGetter.getColumn());
        //second person to use for relation identification
        thingGetter = roleGetter.getThingGetters()[1];
        Assert.assertEquals("person", thingGetter.getConceptType());
        Assert.assertEquals("entity", thingGetter.getHandler().toString());
        thingThingGetter = thingGetter.getThingGetters()[0];
        Assert.assertEquals("phone-number", thingThingGetter.getConceptType());
        Assert.assertEquals("ownership", thingThingGetter.getHandler().toString());
        Assert.assertEquals("peer_2", thingThingGetter.getColumn());

        //appendAttributes
        String appendAttribute = "append-twitter";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/append-twitter-nickname.csv", dc.getAppendAttribute().get(appendAttribute).getDataPaths()[0]);
        Assert.assertEquals(',', dc.getAppendAttribute().get(appendAttribute).getConfig().getSeparator().charValue());
        Assert.assertEquals(50, dc.getAppendAttribute().get(appendAttribute).getConfig().getRowsPerCommit().intValue());
        Assert.assertEquals(2, dc.getAppendAttribute().get(appendAttribute).getAttributes().length);

        Assert.assertEquals("entity", dc.getAppendAttribute().get(appendAttribute).getThingGetter().getHandler().toString());
        Assert.assertEquals("person", dc.getAppendAttribute().get(appendAttribute).getThingGetter().getConceptType());
        Assert.assertEquals(1, dc.getAppendAttribute().get(appendAttribute).getThingGetter().getThingGetters().length);

        Assert.assertEquals("phone-number", dc.getAppendAttribute().get(appendAttribute).getThingGetter().getThingGetters()[0].getConceptType());
        Assert.assertEquals("ownership", dc.getAppendAttribute().get(appendAttribute).getThingGetter().getThingGetters()[0].getHandler().toString());
        Assert.assertEquals("phone_number", dc.getAppendAttribute().get(appendAttribute).getThingGetter().getThingGetters()[0].getColumn());

        attributes = dc.getAppendAttribute().get(appendAttribute).getAttributes();
        Assert.assertEquals("twitter-username", attributes[0].getConceptType());
        Assert.assertNull(attributes[0].getConceptValueType());
        Assert.assertEquals("twitter", attributes[0].getColumn());
        Assert.assertTrue(attributes[0].getRequireNonEmpty());
        Assert.assertNull(attributes[0].getPreprocessorConfig());

    }

}
