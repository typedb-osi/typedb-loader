package insert;

import grakn.client.Grakn;
import grakn.client.api.GraknClient;
import grakn.client.api.GraknSession;
import grakn.client.api.GraknTransaction;
import graql.lang.Graql;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlInsert;
import migrator.EntryMigrationConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static util.Util.loadSchemaFromFile;

public class GraknInserter {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private final String schemaPath;
    private final String databaseName;
    private final String graknURI;

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
        GraknSession schemaSession = getSchemaSession(client);
        GraqlDefine q = Graql.parseQuery(schemaAsString);


        GraknTransaction writeTransaction = schemaSession.transaction(GraknTransaction.Type.WRITE);
        writeTransaction.query().define(q);
        writeTransaction.commit();
        writeTransaction.close();
        schemaSession.close();

        appLogger.info("Defined schema to database <" + databaseName + ">");
    }

    public void matchInsertThreadedInserting(HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> statements, GraknSession session, int threads, int batchSize) throws InterruptedException {

        AtomicInteger queryIndex = new AtomicInteger(0);
        Thread[] ts = new Thread[threads];

        Runnable matchInsertThread =
                () -> {
                    ArrayList<ArrayList<ThingVariable<?>>> matchStatements = statements.get("match");
                    ArrayList<ArrayList<ThingVariable<?>>> insertStatements = statements.get("insert");

                    while (queryIndex.get() < matchStatements.size()) {
                        try (GraknTransaction tx = session.transaction(GraknTransaction.Type.WRITE)) {
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

    public void insertThreadedInserting(ArrayList<ThingVariable<?>> statements, GraknSession session, int threads, int batchSize, EntryMigrationConfig conf) throws InterruptedException {

        AtomicInteger queryIndex = new AtomicInteger(0);
        Thread[] ts = new Thread[threads];

        Runnable insertThread =
                () -> {
                    while (queryIndex.get() < statements.size()) {
                        try (GraknTransaction tx = session.transaction(GraknTransaction.Type.WRITE)) {
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

    public void appendOrInsertThreadedInserting(HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> statements, GraknSession session, int threads, int batchSize) throws InterruptedException {

        AtomicInteger queryIndex = new AtomicInteger(0);
        Thread[] ts = new Thread[threads];

        Runnable matchInsertThread =
                () -> {
                    ArrayList<ArrayList<ThingVariable<?>>> matchStatements = statements.get("match");
                    ArrayList<ArrayList<ThingVariable<?>>> insertStatements = statements.get("insert");

                    while (queryIndex.get() < matchStatements.size()) {
                        try (GraknTransaction tx = session.transaction(GraknTransaction.Type.WRITE)) {
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

//    private HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> convertInsertQuery(ArrayList<ThingVariable<?>> queries, EntryMigrationConfig conf) {
//        if (conf.getSchemaTypeKey() != null) {
//            HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> statements = new HashMap<>();
//            ArrayList<ArrayList<ThingVariable<?>>> matchStatements = new ArrayList<>();
//            ArrayList<ArrayList<ThingVariable<?>>> insertStatements = new ArrayList<>();
//
//            for (ThingVariable<?> insert : queries) {
//                ArrayList<ThingConstraint> insertConstraints = new ArrayList<>();
//                for (ThingConstraint constraint : insert.constraints()) {
//                    if (constraint.isHas() && constraint.toString().contains(conf.getSchemaTypeKey())) {
//                        ThingVariable<?> matchStatement = var(insert.name()).isa(conf.getPce().getSchemaType()).constrain(constraint.asHas());
//                        ArrayList<ThingVariable<?>> matchStatementList = new ArrayList<>();
//                        matchStatementList.add(matchStatement);
//                        matchStatements.add(matchStatementList);
//                    }
//                    insertConstraints.add(constraint);
//                }
//                ThingVariable<?> insertWithoutKey = var(insert.name()).isa(conf.getPce().getSchemaType());
//                for (ThingConstraint constraint : insertConstraints) {
//                    if (constraint.isHas() && !constraint.toString().contains(conf.getSchemaTypeKey())) {
//                        insertWithoutKey.constrain(constraint.asHas());
//                    }
//                }
//                insertStatements.add(new ArrayList<>(Collections.singletonList(insertWithoutKey)));
//            }
//            statements.put("match", matchStatements);
//            statements.put("insert", insertStatements);
//            return statements;
//        } else {
//            return null;
//        }
//    }

    // Utility functions
    public GraknSession getDataSession(GraknClient client) {
        return client.session(databaseName, GraknSession.Type.DATA);
    }

    public GraknSession getSchemaSession(GraknClient client) {
        return client.session(databaseName, GraknSession.Type.SCHEMA);
    }

    public GraknClient getClient() {
        return Grakn.coreClient(graknURI);
    }

    private void deleteDatabaseIfExists(GraknClient client) {
        if (client.databases().contains(databaseName)) {
            client.databases().get(databaseName).delete();
        }
    }

    // used by <grami update> command
    public void loadAndDefineSchema(GraknClient client) {
        String schema = loadSchemaFromFile(schemaPath);
        defineToGrakn(schema, client);
    }
}
