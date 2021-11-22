/*
 * Copyright (C) 2021 Bayer AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config;

import util.Util;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class ConfigurationTest {

    @Test
    public void dcPhoneCallsTest() {
        Configuration dc = Util.initializeConfig(new File("src/test/resources/1.0.0/phoneCalls/dc.json").getAbsolutePath());
        //general
        Assert.assertNotNull(dc);

        //default config
        Assert.assertEquals(',', dc.getGlobalConfig().getSeparator().charValue());

        //attributes
        String att = "is-in-use";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/is-in-use.csv", dc.getAttributes().get(att).getData()[0]);
        Assert.assertEquals("values", dc.getAttributes().get(att).getInsert().getColumn());
        Assert.assertEquals("is-in-use", dc.getAttributes().get(att).getInsert().getAttribute());
        Assert.assertEquals(',', dc.getAttributes().get(att).getConfig().getSeparator().charValue());
        Assert.assertNull(dc.getAttributes().get(att).getInsert().getConceptValueType());
        Assert.assertNull(dc.getAttributes().get(att).getInsert().getListSeparator());
        Assert.assertNull(dc.getAttributes().get(att).getInsert().getPreprocessorConfig());
        Assert.assertEquals(50, dc.getAttributes().get(att).getConfig().getRowsPerCommit().intValue());

        //entities
        String entity = "person";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/person.csv", dc.getEntities().get(entity).getData()[0]);
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/person_nextfile.csv", dc.getEntities().get(entity).getData()[1]);
        Assert.assertEquals(',', dc.getEntities().get(entity).getConfig().getSeparator().charValue());
        Assert.assertEquals("person", dc.getEntities().get(entity).getInsert().getEntity());
        Assert.assertEquals(50, dc.getEntities().get(entity).getConfig().getRowsPerCommit().intValue());
        Configuration.ConstrainingAttribute[] attributes = dc.getEntities().get(entity).getInsert().getOwnerships();
        Assert.assertEquals("first-name", attributes[0].getAttribute());
        Assert.assertNull(attributes[0].getConceptValueType());
        Assert.assertEquals("first_name", attributes[0].getColumn());
        Assert.assertFalse(attributes[0].getRequired());
        Assert.assertNull(attributes[0].getListSeparator());
        Assert.assertNull(attributes[0].getPreprocessorConfig());
        Assert.assertTrue(attributes[2].getRequired());
        Assert.assertEquals(";", attributes[5].getListSeparator());

        //entity-relations
        String relation = "contract";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/contract.csv.gz", dc.getRelations().get(relation).getData()[0]);
        Assert.assertEquals(',', dc.getRelations().get(relation).getConfig().getSeparator().charValue());
        Assert.assertEquals("contract", dc.getRelations().get(relation).getInsert().getRelation());
        Assert.assertEquals(50, dc.getRelations().get(relation).getConfig().getRowsPerCommit().intValue());
        Assert.assertNull(dc.getRelations().get(relation).getInsert().getOwnerships());

        Configuration.Player[] players = dc.getRelations().get(relation).getInsert().getPlayers();
        Configuration.Player player = players[0];
        Assert.assertEquals("provider", player.getRole());
        Assert.assertTrue(player.getRequired());
        Configuration.RoleGetter roleMatch = player.getMatch();
        Assert.assertEquals("company", roleMatch.getType());
        Configuration.ConstrainingAttribute consAtt = roleMatch.getOwnerships()[0];
        Assert.assertEquals("name", consAtt.getAttribute());
        Assert.assertEquals("company_name", consAtt.getColumn());
        Assert.assertNull(consAtt.getListSeparator());
        Assert.assertTrue(consAtt.getRequired());

        player = players[1];
        Assert.assertEquals("customer", player.getRole());
        Assert.assertTrue(player.getRequired());
        roleMatch = player.getMatch();
        Assert.assertEquals("person", roleMatch.getType());
        consAtt = player.getMatch().getOwnerships()[0];
        Assert.assertEquals("phone-number", consAtt.getAttribute());
        Assert.assertEquals("person_id", consAtt.getColumn());
        Assert.assertEquals("###", consAtt.getListSeparator());

        relation = "call";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/call.csv", dc.getRelations().get(relation).getData()[0]);
        Assert.assertEquals(',', dc.getRelations().get(relation).getConfig().getSeparator().charValue());
        Assert.assertEquals(50, dc.getRelations().get(relation).getConfig().getRowsPerCommit().intValue());
        Assert.assertEquals("call", dc.getRelations().get(relation).getInsert().getRelation());

        players = dc.getRelations().get(relation).getInsert().getPlayers();
        player = players[0];
        Assert.assertEquals("caller", player.getRole());
        Assert.assertTrue(player.getRequired());
        roleMatch = player.getMatch();
        Assert.assertEquals("person", roleMatch.getType());
        consAtt = player.getMatch().getOwnerships()[0];
        Assert.assertEquals("phone-number", consAtt.getAttribute());
        Assert.assertEquals("caller_id", consAtt.getColumn());
        Assert.assertNull(consAtt.getListSeparator());

        player = players[1];
        Assert.assertEquals("callee", player.getRole());
        Assert.assertTrue(player.getRequired());
        roleMatch = player.getMatch();
        Assert.assertEquals("person", roleMatch.getType());
        consAtt = player.getMatch().getOwnerships()[0];
        Assert.assertEquals("phone-number", consAtt.getAttribute());
        Assert.assertEquals("callee_id", consAtt.getColumn());
        Assert.assertNull(consAtt.getListSeparator());

        attributes = dc.getRelations().get(relation).getInsert().getOwnerships();
        Configuration.ConstrainingAttribute attribute = attributes[0];
        Assert.assertEquals("started-at", attribute.getAttribute());
        Assert.assertEquals("started_at", attribute.getColumn());
        Assert.assertTrue(attribute.getRequired());
        Assert.assertNull(attribute.getPreprocessorConfig());
        Assert.assertNull(attribute.getListSeparator());

        attribute = attributes[1];
        Assert.assertEquals("duration", attribute.getAttribute());
        Assert.assertEquals("duration", attribute.getColumn());
        Assert.assertTrue(attribute.getRequired());
        Assert.assertNull(attribute.getPreprocessorConfig());
        Assert.assertNull(attribute.getListSeparator());

        //Nested-Relation by Attribute:
        relation = "communication-channel";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/communication-channel.csv", dc.getRelations().get(relation).getData()[0]);
        Assert.assertEquals(',', dc.getRelations().get(relation).getConfig().getSeparator().charValue());
        Assert.assertEquals(50, dc.getRelations().get(relation).getConfig().getRowsPerCommit().intValue());
        Assert.assertEquals("communication-channel", dc.getRelations().get(relation).getInsert().getRelation());

        players = dc.getRelations().get(relation).getInsert().getPlayers();
        player = players[0];
        Assert.assertEquals("peer", player.getRole());
        Assert.assertTrue(player.getRequired());
        roleMatch = player.getMatch();
        Assert.assertEquals("person", roleMatch.getType());
        consAtt = roleMatch.getOwnerships()[0];
        Assert.assertEquals("phone-number", consAtt.getAttribute());
        Assert.assertEquals("peer_1", consAtt.getColumn());
        Assert.assertNull(consAtt.getListSeparator());

        player = players[1];
        Assert.assertEquals("peer", player.getRole());
        Assert.assertTrue(player.getRequired());
        roleMatch = player.getMatch();
        Assert.assertEquals("person", roleMatch.getType());
        consAtt = roleMatch.getOwnerships()[0];
        Assert.assertEquals("phone-number", consAtt.getAttribute());
        Assert.assertEquals("peer_2", consAtt.getColumn());
        Assert.assertNull(consAtt.getListSeparator());

        player = players[2];
        Assert.assertEquals("past-call", player.getRole());
        Assert.assertTrue(player.getRequired());
        roleMatch = player.getMatch();
        Assert.assertEquals("call", roleMatch.getType());
        consAtt = roleMatch.getOwnerships()[0];
        Assert.assertEquals("started-at", consAtt.getAttribute());
        Assert.assertEquals("call_started_at", consAtt.getColumn());
        Assert.assertEquals("###", consAtt.getListSeparator());

        //Nested-Relation by Players:
        relation = "communication-channel-pm";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/communication-channel-pm.csv", dc.getRelations().get(relation).getData()[0]);
        Assert.assertEquals(',', dc.getRelations().get(relation).getConfig().getSeparator().charValue());
        Assert.assertEquals(50, dc.getRelations().get(relation).getConfig().getRowsPerCommit().intValue());
        Assert.assertEquals("communication-channel", dc.getRelations().get(relation).getInsert().getRelation());

        players = dc.getRelations().get(relation).getInsert().getPlayers();
        player = players[0];
        Assert.assertEquals("peer", player.getRole());
        Assert.assertTrue(player.getRequired());
        roleMatch = player.getMatch();
        Assert.assertEquals("person", roleMatch.getType());
        consAtt = roleMatch.getOwnerships()[0];
        Assert.assertEquals("phone-number", consAtt.getAttribute());
        Assert.assertEquals("peer_1", consAtt.getColumn());
        Assert.assertNull(consAtt.getListSeparator());

        player = players[1];
        Assert.assertEquals("peer", player.getRole());
        Assert.assertTrue(player.getRequired());
        roleMatch = player.getMatch();
        Assert.assertEquals("person", roleMatch.getType());
        consAtt = roleMatch.getOwnerships()[0];
        Assert.assertEquals("phone-number", consAtt.getAttribute());
        Assert.assertEquals("peer_2", consAtt.getColumn());
        Assert.assertNull(consAtt.getListSeparator());

        player = players[2];
        Assert.assertEquals("past-call", player.getRole());
        Assert.assertTrue(player.getRequired());
        roleMatch = player.getMatch();
        Assert.assertEquals("call", roleMatch.getType());
        Configuration.Player[] playerPlayers = roleMatch.getPlayers();
        Assert.assertEquals("caller", playerPlayers[0].getRole());
        Assert.assertEquals("callee", playerPlayers[1].getRole());
        Assert.assertEquals("person", playerPlayers[0].getMatch().getType());
        Assert.assertEquals("person", playerPlayers[1].getMatch().getType());
        Assert.assertEquals("peer_1", playerPlayers[0].getMatch().getOwnerships()[0].getColumn());
        Assert.assertEquals("peer_2", playerPlayers[1].getMatch().getOwnerships()[0].getColumn());

        //appendAttributes
        String appendAttribute = "append-twitter";
        Assert.assertEquals("src/test/resources/1.0.0/phoneCalls/append-twitter-nickname.csv", dc.getAppendAttribute().get(appendAttribute).getData()[0]);
        Assert.assertEquals(',', dc.getAppendAttribute().get(appendAttribute).getConfig().getSeparator().charValue());
        Assert.assertEquals(50, dc.getAppendAttribute().get(appendAttribute).getConfig().getRowsPerCommit().intValue());
        Assert.assertEquals(2, dc.getAppendAttribute().get(appendAttribute).getInsert().getOwnerships().length);

        Assert.assertEquals("person", dc.getAppendAttribute().get(appendAttribute).getMatch().getType());
        Assert.assertEquals(1, dc.getAppendAttribute().get(appendAttribute).getMatch().getOwnerships().length);

        Assert.assertEquals("phone-number", dc.getAppendAttribute().get(appendAttribute).getMatch().getOwnerships()[0].getAttribute());
        Assert.assertEquals("phone_number", dc.getAppendAttribute().get(appendAttribute).getMatch().getOwnerships()[0].getColumn());

        attributes = dc.getAppendAttribute().get(appendAttribute).getInsert().getOwnerships();
        Assert.assertEquals("twitter-username", attributes[0].getAttribute());
        Assert.assertNull(attributes[0].getConceptValueType());
        Assert.assertEquals("twitter", attributes[0].getColumn());
        Assert.assertEquals("###", attributes[0].getListSeparator());
        Assert.assertTrue(attributes[0].getRequired());
        Assert.assertNull(attributes[0].getPreprocessorConfig());

    }

}
