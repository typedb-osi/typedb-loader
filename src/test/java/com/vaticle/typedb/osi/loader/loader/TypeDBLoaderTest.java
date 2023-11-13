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

package com.vaticle.typedb.osi.loader.loader;

import com.vaticle.typedb.driver.api.TypeDBDriver;
import com.vaticle.typedb.driver.api.TypeDBSession;
import com.vaticle.typedb.driver.api.TypeDBTransaction;
import com.vaticle.typedb.osi.loader.cli.LoadOptions;
import com.vaticle.typedb.osi.loader.util.TypeDBUtil;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.statement.ThingStatement;
import com.vaticle.typeql.lang.query.TypeQLGet;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

import static com.vaticle.typedb.osi.loader.util.QueryUtilTest.getDT;


public class TypeDBLoaderTest {

    String typeDBUri = "localhost:1729";

    @Test
    public void loadSyntheticTest() {
        String dcPath = new File("src/test/resources/generic/config.json").getAbsolutePath();
        String databaseName = "generic-test";
        String[] args = {
                "load",
                "-c", dcPath,
                "-db", databaseName,
                "-tdb", typeDBUri,
                "-cm"
        };
        TypeDBLoader typeDBLoader = new TypeDBLoader(LoadOptions.parse(args));
        typeDBLoader.load();
    }

    @Test
    public void loadPhoneCallsTest() {
        String dcPath = new File("src/test/resources/phoneCalls/config.json").getAbsolutePath();
        String databaseName = "phone-calls-test";

        String[] args = {
                "load",
                "-c", dcPath,
                "-db", databaseName,
                "-tdb", typeDBUri,
                "-cm",
                "--allowMultiInsert" // TODO we shouldn't be using this to make the tests pass...
        };
        TypeDBLoader typeDBLoader = new TypeDBLoader(LoadOptions.parse(args));
        typeDBLoader.load();

        TypeDBDriver driver = TypeDBUtil.getCoreDriver(typeDBUri);
        TypeDBSession session = TypeDBUtil.getDataSession(driver, databaseName);

        testAttributes(session);
        testEntities(session);
        testRelations(session);
        testAttributeRelation(session);
        testNestedRelations(session);
        testAppendAttribute(session);
        testInsertOrAppend(session);

        session.close();
        driver.close();
    }

    public void testAttributes(TypeDBSession session) {
        try (TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ)) {
            TypeQLGet getQuery = TypeQL.match(TypeQL.cVar("a").isa("is-in-use")).get(TypeQL.cVar("a"));
            Assert.assertEquals(3, read.query().get(getQuery).count());

            getQuery = TypeQL.match(TypeQL.cVar("a").eq("yes").isa("is-in-use")).get(TypeQL.cVar("a"));
            Assert.assertEquals(1, read.query().get(getQuery).count());

            getQuery = TypeQL.match(TypeQL.cVar("a").eq("no").isa("is-in-use")).get(TypeQL.cVar("a"));
            Assert.assertEquals(1, read.query().get(getQuery).count());

            getQuery = TypeQL.match(TypeQL.cVar("a").eq("5").isa("is-in-use")).get(TypeQL.cVar("a"));
            Assert.assertEquals(1, read.query().get(getQuery).count());
        }
    }

    public void testEntities(TypeDBSession session) {

        // query person by phone-number
        try (TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ)) {
            TypeQLGet getQuery = TypeQL.match(TypeQL.cVar("p").isa("person").has("phone-number", "+261 860 539 4754")).get(TypeQL.cVar("p")).limit(1000);
            Assert.assertEquals(1, read.query().get(getQuery).count());

            // query person by last name
            getQuery = TypeQL.match(TypeQL.cVar("p").isa("person").has("last-name", "Smith")).get(TypeQL.cVar("p")).limit(1000);
            Assert.assertEquals(2, read.query().get(getQuery).count());

            // query all entities of type person
            getQuery = TypeQL.match(TypeQL.cVar("c").isa("person")).get(TypeQL.cVar("c")).limit(1000);
            Assert.assertEquals(39, read.query().get(getQuery).count());

            // query all entites of type company
            getQuery = TypeQL.match(TypeQL.cVar("e").isa("company")).get(TypeQL.cVar("e")).limit(1000);
            Assert.assertEquals(2, read.query().get(getQuery).count());
        }
    }

    public void testRelations(TypeDBSession session) {

        // query call by duration
        try (TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ)) {
            TypeQLGet getQuery = TypeQL.match(TypeQL.cVar("c").isa("call").has("duration", 2851)).get(TypeQL.cVar("c")).limit(1000);
            Assert.assertEquals(1, read.query().get(getQuery).count());

            // query call by date
            getQuery = TypeQL.match(TypeQL.cVar("c").isa("call").has("started-at", getDT("2018-09-17T18:43:42"))).get(TypeQL.cVar("c")).limit(1000);
            Assert.assertEquals(1, read.query().get(getQuery).count());

            // query call by caller
            ThingStatement.Thing player = TypeQL.cVar("p").isa("person").has("phone-number", "+7 171 898 0853");
            ThingStatement.Relation relation = TypeQL.cVar("c").rel("caller", TypeQL.cVar("p")).isa("call");
            ArrayList<ThingStatement<?>> statements = new ArrayList<>();
            statements.add(player);
            statements.add(relation);
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("c")).limit(1000);
            Assert.assertEquals(14, read.query().get(getQuery).count());

            // query call by callee
            player = TypeQL.cVar("p").isa("person").has("phone-number", "+7 171 898 0853");
            relation = TypeQL.cVar("c").rel("callee", TypeQL.cVar("p")).isa("call");
            statements = new ArrayList<>();
            statements.add(player);
            statements.add(relation);
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("c")).limit(1000);
            Assert.assertEquals(4, read.query().get(getQuery).count());

            // query call by caller & callee
            ThingStatement.Thing playerOne = TypeQL.cVar("p1").isa("person").has("phone-number", "+7 171 898 0853");
            ThingStatement.Thing playerTwo = TypeQL.cVar("p2").isa("person").has("phone-number", "+57 629 420 5680");
            relation = TypeQL.cVar("c").rel("caller", TypeQL.cVar("p1")).rel("callee", TypeQL.cVar("p2")).isa("call");
            statements = new ArrayList<>();
            statements.add(playerOne);
            statements.add(playerTwo);
            statements.add(relation);
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("c")).limit(1000);
            Assert.assertEquals(4, read.query().get(getQuery).count());
        }
    }

    public void testAttributeRelation(TypeDBSession session) {

        TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ);
        TypeQLGet getQuery = TypeQL.match(TypeQL.cVar("a").isa("in-use")).get(TypeQL.cVar("a"));
        Assert.assertEquals(7, read.query().get(getQuery).count());

        read.close();
    }

    public void testNestedRelations(TypeDBSession session) {

        // query specific communication-channel and count the number of past calls (single past-call):
        try (TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ)) {
            ThingStatement.Thing playerOne = TypeQL.cVar("p1").isa("person").has("phone-number", "+54 398 559 0423");
            ThingStatement.Thing playerTwo = TypeQL.cVar("p2").isa("person").has("phone-number", "+48 195 624 2025");
            ThingStatement.Relation relation = TypeQL.cVar("c").rel("peer", TypeQL.cVar("p1")).rel("peer", TypeQL.cVar("p2")).rel("past-call", TypeQL.cVar("x")).isa("communication-channel");
            ArrayList<ThingStatement<?>> statements = new ArrayList<>();
            statements.add(playerOne);
            statements.add(playerTwo);
            statements.add(relation);
            TypeQLGet getQuery = TypeQL.match(statements).get(TypeQL.cVar("c")).limit(1000);
            Assert.assertEquals(1, read.query().get(getQuery).count());
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("x")).limit(1000);
            Assert.assertEquals(1, read.query().get(getQuery).count());

            // query specific communication-channel and count the number of past calls (listSeparated past-calls:
            playerOne = TypeQL.cVar("p1").isa("person").has("phone-number", "+263 498 495 0617");
            playerTwo = TypeQL.cVar("p2").isa("person").has("phone-number", "+33 614 339 0298");
            relation = TypeQL.cVar("c").rel("peer", TypeQL.cVar("p1")).rel("peer", TypeQL.cVar("p2")).rel("past-call", TypeQL.cVar("x")).isa("communication-channel");
            statements = new ArrayList<>();
            statements.add(playerOne);
            statements.add(playerTwo);
            statements.add(relation);
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("c")).limit(1000);
            Assert.assertEquals(1, read.query().get(getQuery).count());
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("x")).limit(1000);
            Assert.assertEquals(1, read.query().get(getQuery).count());

            // make sure that this doesn't get inserted:
            playerOne = TypeQL.cVar("p1").isa("person").has("phone-number", "+7 690 597 4443");
            playerTwo = TypeQL.cVar("p2").isa("person").has("phone-number", "+54 398 559 9999");
            relation = TypeQL.cVar("c").rel("peer", TypeQL.cVar("p1")).rel("peer", TypeQL.cVar("p2")).rel("past-call", TypeQL.cVar("x")).isa("communication-channel");
            statements = new ArrayList<>();
            statements.add(playerOne);
            statements.add(playerTwo);
            statements.add(relation);
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("c")).limit(1000);
            Assert.assertEquals(0, read.query().get(getQuery).count());
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("x")).limit(1000);
            Assert.assertEquals(0, read.query().get(getQuery).count());

            // these are added by doing player matching for past calls:
            playerOne = TypeQL.cVar("p1").isa("person").has("phone-number", "+81 308 988 7153");
            playerTwo = TypeQL.cVar("p2").isa("person").has("phone-number", "+351 515 605 7915");
            relation = TypeQL.cVar("c").rel("peer", TypeQL.cVar("p1")).rel("peer", TypeQL.cVar("p2")).rel("past-call", TypeQL.cVar("x")).isa("communication-channel");
            statements = new ArrayList<>();
            statements.add(playerOne);
            statements.add(playerTwo);
            statements.add(relation);
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("c")).limit(1000);
            Assert.assertEquals(5, read.query().get(getQuery).count());
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("x")).limit(1000);
            Assert.assertEquals(5, read.query().get(getQuery).count());

            playerOne = TypeQL.cVar("p1").isa("person").has("phone-number", "+7 171 898 0853");
            playerTwo = TypeQL.cVar("p2").isa("person").has("phone-number", "+57 629 420 5680");
            relation = TypeQL.cVar("c").rel("peer", TypeQL.cVar("p1")).rel("peer", TypeQL.cVar("p2")).rel("past-call", TypeQL.cVar("x")).isa("communication-channel");
            statements = new ArrayList<>();
            statements.add(playerOne);
            statements.add(playerTwo);
            statements.add(relation);
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("c")).limit(1000);
            Assert.assertEquals(4, read.query().get(getQuery).count());
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("x")).limit(1000);
            Assert.assertEquals(4, read.query().get(getQuery).count());

            // these must not be found (come from player-matched past-call):
            playerOne = TypeQL.cVar("p1").isa("person").has("phone-number", "+261 860 539 4754");
            relation = TypeQL.cVar("c").rel("peer", TypeQL.cVar("p1")).rel("past-call", TypeQL.cVar("x")).isa("communication-channel");
            statements = new ArrayList<>();
            statements.add(playerOne);
            statements.add(relation);
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("c")).limit(1000);
            Assert.assertEquals(0, read.query().get(getQuery).count());
            getQuery = TypeQL.match(statements).get(TypeQL.cVar("x")).limit(1000);
            Assert.assertEquals(0, read.query().get(getQuery).count());
        }
    }

    public void testAppendAttribute(TypeDBSession session) {

        // Count number of total inserts
        try (TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ)) {
            TypeQLGet.Limited getQuery = TypeQL.match(TypeQL.cVar("p").isa("person").has("twitter-username", TypeQL.cVar("x"))).get(TypeQL.cVar("x")).limit(1000);
            Assert.assertEquals(7, read.query().get(getQuery).count());

            // Count multi-write using listSeparator
            getQuery = TypeQL.match(TypeQL.cVar("p").isa("person").has("phone-number", "+263 498 495 0617").has("twitter-username", TypeQL.cVar("x"))).get(TypeQL.cVar("x")).limit(1000);
            Assert.assertEquals(2, read.query().get(getQuery).count());

            //test relation total inserts
            getQuery = TypeQL.match(TypeQL.cVar("c").isa("call").has("call-rating", TypeQL.cVar("cr"))).get(TypeQL.cVar("c")).limit(1000);
            Assert.assertEquals(5, read.query().get(getQuery).count());

            // specific relation write
            getQuery = TypeQL.match(TypeQL.cVar("c").isa("call").has("started-at", getDT("2018-09-24T03:16:48")).has("call-rating", TypeQL.cVar("cr"))).get(TypeQL.cVar("cr")).limit(1000);
            read.query().get(getQuery).forEach(answer -> Assert.assertEquals(5L, answer.get("cr").asAttribute().getValue().asLong()));
        }
    }

    public void testInsertOrAppend(TypeDBSession session) {
        try (TypeDBTransaction read = session.transaction(TypeDBTransaction.Type.READ)) {
            TypeQLGet getQuery = TypeQL.match(TypeQL.cVar("e").isa("person").has("nick-name", TypeQL.cVar("x"))).get(TypeQL.cVar("x"));
            Assert.assertEquals(12, read.query().get(getQuery).count());

            // test new ones present (middle and at end)
            getQuery = TypeQL.match(TypeQL.cVar("p").isa("person").has("first-name", "Naruto")).get(TypeQL.cVar("p")).limit(1000);
            Assert.assertEquals(1, read.query().get(getQuery).count());

            getQuery = TypeQL.match(TypeQL.cVar("p").isa("person").has("first-name", "Sasuke")).get(TypeQL.cVar("p")).limit(1000);
            Assert.assertEquals(1, read.query().get(getQuery).count());

            getQuery = TypeQL.match(TypeQL.cVar("p").isa("person").has("first-name", "Sakura")).get(TypeQL.cVar("p")).limit(1000);
            Assert.assertEquals(1, read.query().get(getQuery).count());
        }
    }
}
