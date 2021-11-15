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

package generator;

import config.Configuration;
import util.TypeDBUtil;
import util.Util;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class AttributeGeneratorTest {

    @Test
    public void generateInsertStatementsTest() throws IOException {
        String dbName = "attribute-generator-test";
        String sp = new File("src/test/resources/1.0.0/phoneCalls/schema.gql").getAbsolutePath();
        TypeDBClient client = TypeDBUtil.getClient("localhost:1729");
        TypeDBUtil.cleanAndDefineSchemaToDatabase(client, dbName, sp);

        String dp = new File("src/test/resources/1.0.0/phoneCalls/is-in-use.csv").getAbsolutePath();
        String dcp = new File("src/test/resources/1.0.0/phoneCalls/dc.json").getAbsolutePath();
        Configuration dc = Util.initializeDataConfig(dcp);
        assert dc != null;
        String attributeKey = "is-in-use";
        AttributeGenerator gen = new AttributeGenerator(dp,
                dc.getAttributes().get(attributeKey),
                Objects.requireNonNullElseGet(dc.getAttributes().get(attributeKey).getConfig().getSeparator(), () -> dc.getGlobalConfig().getSeparator()));

        TypeDBSession session = TypeDBUtil.getDataSession(client, dbName);
        dc.getAttributes().get(attributeKey).getAttribute().setConceptValueType(session.transaction(TypeDBTransaction.Type.READ));
        session.close();
        client.close();

        Iterator<String> iterator = Util.newBufferedReader(dp).lines().skip(1).iterator();

        String[] row = Util.parseCSV(iterator.next());
        List<TypeQLInsert> insertStatements = gen.generateInsertStatements(row);
        String tmp = "insert $a \"yes\" isa is-in-use;";
        Assert.assertEquals(tmp, insertStatements.get(0).toString());

        row = Util.parseCSV(iterator.next());
        insertStatements = gen.generateInsertStatements(row);
        tmp = "insert $a \"no\" isa is-in-use;";
        Assert.assertEquals(tmp, insertStatements.get(0).toString());

        try {
            Util.parseCSV(iterator.next());
        } catch (IndexOutOfBoundsException indexOutOfBoundsException) {
            Assert.assertEquals("Index 0 out of bounds for length 0", indexOutOfBoundsException.getMessage());
        }

        row = Util.parseCSV(iterator.next());
        insertStatements = gen.generateInsertStatements(row);
        tmp = "insert $a \"5\" isa is-in-use;";
        Assert.assertEquals(tmp, insertStatements.get(0).toString());
    }
}
