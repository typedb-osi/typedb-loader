package write;

import com.vaticle.typedb.client.TypeDB;
import com.vaticle.typedb.client.api.answer.ConceptMap;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typedb.client.common.exception.TypeDBClientException;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import processor.InsertQueries;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static util.Util.loadSchemaFromFile;

public class TypeDBWriter {

    //TODO replace sout with proper logging

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private final String schemaPath;
    private final String databaseName;
    private final String graknURI;

    public TypeDBWriter(String graknURI, String port, String schemaPath, String databaseName) {
        this.schemaPath = schemaPath;
        this.databaseName = databaseName;
        this.graknURI = String.format("%s:%s", graknURI, port);
    }

    // Schema Operations
    public void cleanAndDefineSchemaToDatabase(TypeDBClient client) {
        deleteDatabaseIfExists(client);
        createDatabase(client);
        String schema = loadSchemaFromFile(schemaPath);
        defineToGrakn(schema, client);
    }

    private void createDatabase(TypeDBClient client) {
        client.databases().create(databaseName);
    }

    private void defineToGrakn(String schemaAsString, TypeDBClient client) {
        TypeDBSession schemaSession = getSchemaSession(client);
        TypeQLDefine q = TypeQL.parseQuery(schemaAsString);


        TypeDBTransaction writeTransaction = schemaSession.transaction(TypeDBTransaction.Type.WRITE);
        writeTransaction.query().define(q);
        writeTransaction.commit();
        writeTransaction.close();
        schemaSession.close();

        appLogger.info("Defined schema to database <" + databaseName + ">");
    }

    public void matchInsertThreadedInserting(InsertQueries statements, TypeDBSession session, int threads, int batchSize) throws InterruptedException {

        AtomicInteger queryIndex = new AtomicInteger(0);
        Thread[] ts = new Thread[threads];

        Runnable matchInsertThread =
                () -> {
                    while (queryIndex.get() < statements.getMatchInserts().size()) {
                        try (TypeDBTransaction tx = session.transaction(TypeDBTransaction.Type.WRITE)) {
                            int q;
                            for (int i = 0; i < batchSize && (q = queryIndex.getAndIncrement()) < statements.getMatchInserts().size(); i++) {
                                ArrayList<ThingVariable<?>> rowMatchStatements = statements.getMatchInserts().get(q).getMatches();
                                ThingVariable<?> rowInsertStatements = statements.getMatchInserts().get(q).getInsert();
                                if (rowMatchStatements != null && rowInsertStatements != null) {
                                    TypeQLInsert query = TypeQL.match(rowMatchStatements).insert(rowInsertStatements);
                                    tx.query().insert(query);
                                }
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

    //TODO: make non-blocking (RocksDB busy catch) for append-attribute statements
    public void insertThreadedInserting(ArrayList<ThingVariable<?>> statements, TypeDBSession session, int threads, int batchSize) throws InterruptedException {

        AtomicInteger queryIndex = new AtomicInteger(0);
        Thread[] ts = new Thread[threads];

        Runnable insertThread =
                () -> {
                    while (queryIndex.get() < statements.size()) {
                        try (TypeDBTransaction tx = session.transaction(TypeDBTransaction.Type.WRITE)) {
                            int q;
                            for (int i = 0; i < batchSize && (q = queryIndex.getAndIncrement()) < statements.size(); i++) {
                                if (statements.get(q) != null) {
                                    TypeQLInsert query = TypeQL.insert(statements.get(q));
                                    tx.query().insert(query);
                                }
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

    private static class aoiWriterThread implements Runnable {

        private final AtomicInteger queryIndex;
        private final AtomicBoolean[] done;
        private final int threadId;
        private final InsertQueries statements;
        private final int batchSize;
        private final TypeDBSession session;

        public aoiWriterThread(AtomicInteger queryIndex, AtomicBoolean[] done, int threadId, InsertQueries statements, int batchSize, TypeDBSession session) {
            this.queryIndex = queryIndex;
            this.done = done;
            this.threadId = threadId;
            this.statements = statements;
            this.batchSize = batchSize;
            this.session = session;
        }

        public void run() {
            while (queryIndex.get() < statements.getMatchInserts().size()) {
                try (TypeDBTransaction tx = session.transaction(TypeDBTransaction.Type.WRITE)) {
                    int q;
                    ArrayList<TypeQLInsert> catchQueries = new ArrayList<>();
                    for (int i = 0; i < batchSize && (q = queryIndex.getAndIncrement()) < statements.getMatchInserts().size(); i++) {
                        ThingVariable<?> directInsert = statements.getDirectInserts().get(q);
                        ArrayList<ThingVariable<?>> matchInsertMatches = statements.getMatchInserts().get(q).getMatches();
                        ThingVariable<?> matchInsertInsert = statements.getMatchInserts().get(q).getInsert();
                        // if matchInserts contains nulls - do direct write
                        if (statements.getMatchInserts().get(q).getMatches() == null) {
                            if (directInsert != null) {
                                TypeQLInsert query = TypeQL.insert(directInsert);
                                tx.query().insert(query);
                                catchQueries.add(query);
//                                        System.out.println("Direct Insert finished because getMatches is null: " + query);
                            }
                        } else {
                            // else try to write match-write - if the result is 0, do direct write
                            if (matchInsertMatches != null && matchInsertInsert != null) {
                                TypeQLInsert query = TypeQL.match(matchInsertMatches).insert(matchInsertInsert);
                                final Stream<ConceptMap> insertedStream = tx.query().insert(query);
                                catchQueries.add(query);
                                if (insertedStream.count() == 0) {
                                    if (directInsert != null) {
                                        query = TypeQL.insert(directInsert);
                                        tx.query().insert(query);
                                        catchQueries.add(query);
//                                                System.out.println("Failed match insert query. Inserted using direct insert: " + query);
                                    }
                                } else {
//                                            System.out.println("Inserted using match insert query. Query was: " + query.toString());
                                }
                            }
                        }
                    }
                    try {
                        tx.commit();
                    } catch (TypeDBClientException typeDBClientException) {
//                        System.out.println("got a failure!!! in thread " + threadId);
                        insertAndCommit(session, catchQueries);
                    }
                    done[threadId] = new AtomicBoolean(true);
                }
            }
        }
    }

    public static void insertAndCommit(TypeDBSession session, ArrayList<TypeQLInsert> queries) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try (TypeDBTransaction txn = session.transaction(TypeDBTransaction.Type.WRITE)) {
            for (TypeQLInsert q : queries) {
                txn.query().insert(q);
            }
            try {
                txn.commit();
            } catch (TypeDBClientException typeDBClientException) {
                if (typeDBClientException.toString().contains("org.rocksdb.RocksDBException: Busy")) {
                    insertAndCommit(session, queries);
                }
            }
        }
    }

    public void appendOrInsertThreadedInserting(InsertQueries statements, TypeDBSession session, int threads, int batchSize) throws InterruptedException {

        AtomicInteger queryIndex = new AtomicInteger(0);
        AtomicBoolean[] done = new AtomicBoolean[threads];
        Thread[] ts = new Thread[threads];

        for (int i = 0; i < ts.length; i++) {
            done[i] = new AtomicBoolean(false);
            ts[i] = new Thread(new aoiWriterThread(queryIndex, done, i, statements, batchSize, session));
        }
        for (Thread thread : ts) {
            thread.start();
        }
        for (Thread thread : ts) {
            thread.join();
        }
    }

//    public void appendOrInsertThreadedInserting(InsertQueries statements, TypeDBSession session, int threads, int batchSize) throws InterruptedException {
//
//        AtomicInteger queryIndex = new AtomicInteger(0);
//        Thread[] ts = new Thread[threads];
//
//        Runnable matchInsertThread =
//                () -> {
//                    while (queryIndex.get() < statements.getMatchInserts().size()) {
//                        try (TypeDBTransaction tx = session.transaction(TypeDBTransaction.Type.WRITE)) {
//                            int q;
//                            ArrayList<TypeQLInsert> catchQueries = new ArrayList<>();
//                            for (int i = 0; i < batchSize && (q = queryIndex.getAndIncrement()) < statements.getMatchInserts().size(); i++) {
//                                ThingVariable<?> directInsert = statements.getDirectInserts().get(q);
//                                ArrayList<ThingVariable<?>> matchInsertMatches = statements.getMatchInserts().get(q).getMatches();
//                                ThingVariable<?> matchInsertInsert = statements.getMatchInserts().get(q).getInsert();
//                                // if matchInserts contains nulls - do direct write
//                                if (statements.getMatchInserts().get(q).getMatches() == null) {
//                                    if (directInsert != null) {
//                                        TypeQLInsert query = TypeQL.insert(directInsert);
//                                        tx.query().insert(query);
//                                        catchQueries.add(query);
////                                        System.out.println("Direct Insert finished because getMatches is null: " + query);
//                                    }
//                                } else {
//                                    // else try to write match-write - if the result is 0, do direct write
//                                    if (matchInsertMatches != null && matchInsertInsert != null) {
//                                        TypeQLInsert query = TypeQL.match(matchInsertMatches).insert(matchInsertInsert);
//                                        final Stream<ConceptMap> insertedStream = tx.query().insert(query);
//                                        if (insertedStream.count() == 0) {
//                                            if (directInsert != null) {
//                                                query = TypeQL.insert(directInsert);
//                                                tx.query().insert(query);
//                                                catchQueries.add(query);
////                                                System.out.println("Failed match insert query. Inserted using direct insert: " + query);
//                                            }
//                                        } else {
////                                            System.out.println("Inserted using match insert query. Query was: " + query.toString());
//                                        }
//                                    }
//                                }
//                            }
//                            tx.commit();
//                        }
//                    }
//                };
//
//        for (int i = 0; i < ts.length; i++) {
//            ts[i] = new Thread(matchInsertThread);
//        }
//        for (Thread thread : ts) {
//            thread.start();
//        }
//        for (Thread thread : ts) {
//            thread.join();
//        }
//    }

    // Utility functions
    public TypeDBSession getDataSession(TypeDBClient client) {
        return client.session(databaseName, TypeDBSession.Type.DATA);
    }

    public TypeDBSession getSchemaSession(TypeDBClient client) {
        return client.session(databaseName, TypeDBSession.Type.SCHEMA);
    }

    public TypeDBClient getClient() {
        return TypeDB.coreClient(graknURI);
    }

    private void deleteDatabaseIfExists(TypeDBClient client) {
        if (client.databases().contains(databaseName)) {
            client.databases().get(databaseName).delete();
        }
    }

    // used by <grami update> command
    public void loadAndDefineSchema(TypeDBClient client) {
        String schema = loadSchemaFromFile(schemaPath);
        defineToGrakn(schema, client);
    }
}
