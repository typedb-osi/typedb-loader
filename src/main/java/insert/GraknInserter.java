package insert;

import grakn.client.GraknClient;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.statement.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static graql.lang.Graql.parse;

public class GraknInserter {

    private final String schemaPath;
    private final String keyspaceName;
    private final String uri;
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");

    public GraknInserter(String uri, String port, String schemaPath, String keyspaceName) {
        this.schemaPath = schemaPath;
        this.keyspaceName = keyspaceName;
        this.uri = String.format("%s:%s", uri, port);
    }

    // Schema Operations
    public GraknClient.Session setKeyspaceToSchema(GraknClient client, GraknClient.Session session) {
        if (client.keyspaces().retrieve().contains(keyspaceName)) {
            deleteKeyspace(client);
            session = getSession(client);
        }
        String schema = loadSchemaFromFile();
        defineToGrakn(schema, session);
        return session;
    }

    public GraknClient.Session updateCurrentSchema(GraknClient client, GraknClient.Session session) {
        String schema = loadSchemaFromFile();
        defineToGrakn(schema, session);
        return session;
    }

    public String loadSchemaFromFile() {
        String schema="";
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(this.schemaPath)));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line).append("\n");
                line = br.readLine();
            }
            schema = sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return schema;
    }

    private void defineToGrakn(String insertString, GraknClient.Session session) {
        GraknClient.Transaction writeTransaction = session.transaction().write();
        writeTransaction.execute((GraqlDefine) parse(insertString));
        writeTransaction.commit();
        appLogger.info("Successfully defined schema");
    }

    // Entity Operations
    public int insertEntityToGrakn(ArrayList<Statement> statements, GraknClient.Session session) {
        GraknClient.Transaction writeTransaction = session.transaction().write();
        int i = 0;
        for (Statement st : statements) {
            writeTransaction.execute(Graql.insert(st));
            i++;
        }
        writeTransaction.commit();
        appLogger.trace(String.format("Txn with ID: %s has committed and is closed", writeTransaction.toString()));
        return i;
    }

    public void futuresParallelInsertEntity(ArrayList<Statement> insertStatements, GraknClient.Session session, int cores) throws ExecutionException, InterruptedException {
        ArrayList<ArrayList<Statement>> batches = createEvenEntityBatches(insertStatements, cores);

        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (ArrayList<Statement> batch : batches) {
            if (!batch.isEmpty()) {
                CompletableFuture<Integer> inserted = CompletableFuture.supplyAsync(() ->  insertEntityToGrakn(batch, session));
                futures.add(inserted);
            }
        }
        for (CompletableFuture<Integer> f : futures) {
            f.get();
        }
    }

    private static ArrayList<ArrayList<Statement>> createEvenEntityBatches(ArrayList<Statement> inserts, int cores) {

        ArrayList<ArrayList<Statement>> batches = new ArrayList<>();
        ArrayList<String> batchStrings = new ArrayList<>();

        for (int i = 0; i < cores; i++) {
            ArrayList<Statement> batch = new ArrayList<>();
            batches.add(batch);
            batchStrings.add("");
        }

        for (Statement insert: inserts) {
            int shortest = getListIndexOfShortestString(batchStrings);
            batches.get(shortest).add(insert);
            String curString = batchStrings.get(shortest);
            batchStrings.set(shortest, curString + insert.toString());
        }

        appLogger.trace("entity batches.size: " + batches.size());
        appLogger.trace("bucket sizes:");
        for (String s : batchStrings) {
            appLogger.trace(s.length());
        }
        return batches;
    }

    // Relation Operations
    public int insertMatchInsertToGrakn(ArrayList<ArrayList<ArrayList<Statement>>> statements, GraknClient.Session session) {

        ArrayList<ArrayList<Statement>> matchStatements = statements.get(0);
        ArrayList<ArrayList<Statement>> insertStatements = statements.get(1);

        GraknClient.Transaction writeTransaction = session.transaction().write();
        int i = 0;
        for (int row = 0; row < matchStatements.size(); row++) {
            ArrayList<Statement> rowMatchStatements = matchStatements.get(row);
            ArrayList<Statement> rowInsertStatements = insertStatements.get(row);
            writeTransaction.execute(Graql.match(rowMatchStatements).insert(rowInsertStatements));
            i++;
        }
        writeTransaction.commit();
        appLogger.trace(String.format("Txn with ID: %s has committed and is closed", writeTransaction.toString()));
        return i;
    }

    public void futuresParallelInsertMatchInsert(ArrayList<ArrayList<ArrayList<Statement>>> statements, GraknClient.Session session, int cores) throws ExecutionException, InterruptedException {
        ArrayList<ArrayList<ArrayList<ArrayList<Statement>>>> batches = createEvenMatchInsertBatches(statements, cores);

        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (ArrayList<ArrayList<ArrayList<Statement>>> batch : batches) {
            if (!batch.isEmpty()) {
                CompletableFuture<Integer> inserted = CompletableFuture.supplyAsync(() -> insertMatchInsertToGrakn(batch, session));
                futures.add(inserted);
            }
        }
        for (CompletableFuture<Integer> f : futures) {
            f.get();
        }
    }

    private static ArrayList<ArrayList<ArrayList<ArrayList<Statement>>>> createEvenMatchInsertBatches(ArrayList<ArrayList<ArrayList<Statement>>> statements, int cores) {

        ArrayList<ArrayList<ArrayList<ArrayList<Statement>>>> batches = new ArrayList<>();
        ArrayList<String> batchStrings = new ArrayList<>();

        ArrayList<ArrayList<Statement>> matchStatements = statements.get(0);
        ArrayList<ArrayList<Statement>> insertStatements = statements.get(1);

        for (int i = 0; i < cores; i++) {
            ArrayList<ArrayList<Statement>> matches = new ArrayList<>();
            ArrayList<ArrayList<Statement>> inserts = new ArrayList<>();

            ArrayList<ArrayList<ArrayList<Statement>>> batch = new ArrayList<>();
            batch.add(matches);
            batch.add(inserts);

            batches.add(batch);
            batchStrings.add("");
        }

        for (int i = 0; i < matchStatements.size(); i++) {
            int shortestBatchIndex = getListIndexOfShortestString(batchStrings);
            // add matches
            batches.get(shortestBatchIndex).get(0).add(matchStatements.get(i));
            // add inserts
            batches.get(shortestBatchIndex).get(1).add(insertStatements.get(i));
            // update string for batchIndex
            String curString = batchStrings.get(shortestBatchIndex);
            String mat = matchStatements.get(i).stream().map(Statement::toString).collect(Collectors.joining(";"));
            mat += insertStatements.get(i).get(0).toString();
            batchStrings.set(shortestBatchIndex, curString + mat);
        }

        appLogger.trace("relation batches.size: " + batches.size());
        appLogger.trace("bucket sizes:");
        for (String s : batchStrings) {
            appLogger.trace(s.length());
        }

        return batches;
    }

    // Utility functions
    public GraknClient.Session getSession(GraknClient client) {
        return client.session(this.keyspaceName);
    }

    public GraknClient getClient() {
        return new GraknClient(this.uri);
    }

    private void deleteKeyspace(GraknClient client) {
        client.keyspaces().delete(this.keyspaceName);
    }

    private static int getListIndexOfShortestString(ArrayList<String> batchStrings) {
        int shortest = 0;
        for (String s : batchStrings) {
            if (s.length() < batchStrings.get(shortest).length()) {
                shortest = batchStrings.indexOf(s);
            }
        }
        return shortest;
    }
}
