package insert;

import grakn.client.GraknClient;
import grakn.client.GraknClient.Transaction;
import grakn.client.GraknClient.Session;
import graql.lang.Graql;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlInsert;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static util.Util.loadSchemaFromFile;

public class GraknInserter {

    private final String schemaPath;
    private final String databaseName;
    private final String graknURI;
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");

    public GraknInserter(String graknURI, String port, String schemaPath, String databaseName) {
        this.schemaPath = schemaPath;
        this.databaseName = databaseName;
        this.graknURI = String.format("%s:%s", graknURI, port);
    }

    // Schema Operations
    public void cleanAndDefineSchemaToDatabase(GraknClient client) {
        deleteDatabaseIfExists(client);
        createDatabase(client);
        String schema = loadSchemaFromFile(schemaPath);
        defineToGrakn(schema, client);
    }

    private void createDatabase(GraknClient client) {
        client.databases().create(databaseName);
    }

    private void defineToGrakn(String schemaAsString, GraknClient client) {
        Session schemaSession = getSchemaSession(client);
        GraqlDefine q = Graql.parseQuery(schemaAsString);


        Transaction writeTransaction = schemaSession.transaction(Transaction.Type.WRITE);
        writeTransaction.query().define(q);
        writeTransaction.commit();
        writeTransaction.close();
        schemaSession.close();

        appLogger.info("Defined schema to database <" + databaseName + ">");
    }

    public void matchInsertThreadedInserting(HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> statements, Session session, int threads, int batchSize) throws InterruptedException {

        AtomicInteger queryIndex = new AtomicInteger(0);
        Thread[] ts = new Thread[threads];

        Runnable matchInsertThread =
                () -> {
                    ArrayList<ArrayList<ThingVariable<?>>> matchStatements = statements.get("match");
                    ArrayList<ArrayList<ThingVariable<?>>> insertStatements = statements.get("insert");

                    while (queryIndex.get() < matchStatements.size()) {
                        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                            int q;
                            for (int i = 0; i < batchSize && (q = queryIndex.getAndIncrement()) < matchStatements.size(); i++) {
                                ArrayList<ThingVariable<?>> rowMatchStatements = matchStatements.get(q);
                                ArrayList<ThingVariable<?>> rowInsertStatements = insertStatements.get(q);
                                GraqlInsert query = Graql.match(rowMatchStatements).insert(rowInsertStatements);
                                tx.query().insert(query);
                            }
                            tx.commit();
                        }
                    }
                };

        for (int i = 0; i < ts.length; i++) {
            ts[i] = new Thread(matchInsertThread);
        }
        for (Thread value : ts) {
            value.start();
        }
        for (Thread thread : ts) {
            thread.join();
        }
    }

    public void insertThreadedInserting(ArrayList<ThingVariable<?>> statements, Session session, int threads, int batchSize) throws InterruptedException {

        AtomicInteger queryIndex = new AtomicInteger(0);
        Thread[] ts = new Thread[threads];

        Runnable insertThread =
                () -> {
                    while (queryIndex.get() < statements.size()) {
                        try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                            int q;
                            for (int i = 0; i < batchSize && (q = queryIndex.getAndIncrement()) < statements.size(); i++) {
                                GraqlInsert query = Graql.insert(statements.get(q));
                                tx.query().insert(query);
                            }
                            tx.commit();
                        }
                    }
                };
        for (int i = 0; i < ts.length; i++) {
            ts[i] = new Thread(insertThread);
        }
        for (Thread value : ts) {
            value.start();
        }
        for (Thread thread : ts) {
            thread.join();
        }
    }

    // Utility functions
    public Session getDataSession(GraknClient client) {
        return client.session(databaseName, Session.Type.DATA);
    }

    public Session getSchemaSession(GraknClient client) {
        return client.session(databaseName, Session.Type.SCHEMA);
    }

    public GraknClient getClient() {
        return GraknClient.core(graknURI);
    }

    private void deleteDatabaseIfExists(GraknClient client) {
        if (client.databases().contains(databaseName)) {
            client.databases().delete(databaseName);
        }
    }

    // used by <grami update> command
    public void loadAndDefineSchema(GraknClient client) {
        String schema = loadSchemaFromFile(schemaPath);
        defineToGrakn(schema, client);
    }
}
