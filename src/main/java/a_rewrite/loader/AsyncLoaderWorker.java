package a_rewrite.loader;

import a_rewrite.config.Configuration;
import a_rewrite.generator.AttributeGenerator;
import a_rewrite.generator.EntityGenerator;
import a_rewrite.generator.Generator;
import a_rewrite.generator.RelationGenerator;
import a_rewrite.util.TypeDBUtil;
import a_rewrite.util.Util;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncLoaderWorker {

    private static final DecimalFormat countFormat = new DecimalFormat("#,###");
    private static final DecimalFormat decimalFormat = new DecimalFormat("#,###.00");
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    public final ExecutorService executor;
    private final int threads = Runtime.getRuntime().availableProcessors();
    private final String databaseName;
    private final AtomicBoolean hasError;
    private final int batchGroup;
    private final Configuration dc;

    public AsyncLoaderWorker(Configuration dc, String databaseName) {
        this.dc = dc;
        this.databaseName = databaseName;
        hasError = new AtomicBoolean(false);
        this.batchGroup = 8;
        this.executor = Executors.newFixedThreadPool(threads, new NamedThreadFactory(databaseName));
    }

    public void run(TypeDBClient client) throws IOException, InterruptedException {

        try (TypeDBSession session = TypeDBUtil.getDataSession(client, databaseName)) {
            // Load attributes
            Util.info("loading attributes");
            if (dc.getAttributes() != null) {
                for (Map.Entry<String, Configuration.Attribute> attribute : dc.getAttributes().entrySet()) {
                    for (String fp : attribute.getValue().getDataPaths()) {
                        Util.setAttributeConceptType(attribute.getValue(), session);
                        Generator gen = new AttributeGenerator(fp, attribute.getValue(), getSeparator(attribute.getValue().getSeparator()));
                        if (!hasError.get())
                            asyncLoad(session, fp, gen, getRowsPerCommit(attribute.getValue().getRowsPerCommit()));
                    }
                }
            }

            // Load entities
            Util.info("loading entities");
            if (dc.getEntities() != null) {
                for (Map.Entry<String, Configuration.Entity> entity : dc.getEntities().entrySet()) {
                    for (String fp : entity.getValue().getDataPaths()) {
                        for (int idx = 0; idx < entity.getValue().getAttributes().length; idx++) {
                            Util.setEntityHasAttributeConceptType(entity.getValue(), idx, session);
                        }
                        Generator gen = new EntityGenerator(fp, entity.getValue(), getSeparator(entity.getValue().getSeparator()));
                        if (!hasError.get()) asyncLoad(session, fp, gen, getRowsPerCommit(entity.getValue().getRowsPerCommit()));
                    }
                }
            }

//            //Load relations
            Util.info("loading relations");
            if (dc.getRelations() != null) {
                for (Map.Entry<String, Configuration.Relation> relation : dc.getRelations().entrySet()) {
                    for (String fp : relation.getValue().getDataPaths()) {
                        if(relation.getValue().getAttributes() != null) {
                            for (int idx = 0; idx < relation.getValue().getAttributes().length; idx++) {
                                Util.setRelationHasAttributeConceptType(relation.getValue(), idx, session);
                            }
                        }
                        for (int idx = 0; idx < relation.getValue().getPlayers().length; idx++) {
                            Util.setGetterAttributeConceptType(relation.getValue(), idx, session);
                        }
                        Generator gen = new RelationGenerator(fp, relation.getValue(), getSeparator(relation.getValue().getSeparator()));
                        if (!hasError.get()) {
                            asyncLoad(session, fp, gen, getRowsPerCommit(relation.getValue().getRowsPerCommit()));
                        }
                    }
                }
            }

            //Finished
            Util.info("TypeDB Loader finished");
        }
    }


    private int getRowsPerCommit(Integer rowsPerCommit) {
        return Objects.requireNonNullElseGet(rowsPerCommit, () -> dc.getDefaultConfig().getRowsPerCommit());
    }

    private Character getSeparator(Character separator) {
        return Objects.requireNonNullElseGet(separator, () -> dc.getDefaultConfig().getSeparator());
    }

    private void asyncLoad(TypeDBSession session,
                           String filename,
                           Generator gen,
                           int batch) throws IOException, InterruptedException {
        Util.info("async-migrate (start): {}", filename);
        LinkedBlockingQueue<Either<List<List<String[]>>, Done>> queue = new LinkedBlockingQueue<>(threads * 4);
        List<CompletableFuture<Void>> asyncWrites = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            asyncWrites.add(asyncWrite(i + 1, filename, gen, session, queue));
        }
        bufferedRead(filename, gen, batch, queue);
        CompletableFuture.allOf(asyncWrites.toArray(new CompletableFuture[0])).join();
        Util.info("async-migrate (end): {}", filename);
    }

    private void bufferedRead(String filename,
                              Generator gen,
                              int batch,
                              LinkedBlockingQueue<Either<List<List<String[]>>, AsyncLoaderWorker.Done>> queue) throws InterruptedException, IOException {
        Iterator<String> iterator = Util.newBufferedReader(filename).lines().skip(1).iterator();
        List<List<String[]>> rowGroups = new ArrayList<>(batchGroup);
        List<String[]> rows = new ArrayList<>(batch);

        int count = 0;
        Instant startRead = Instant.now();
        Instant startBatch = Instant.now();
        while (iterator.hasNext() && !hasError.get()) {
            count++;
            String[] rowTokens;
            try {
                rowTokens = Util.parseBySeparator(iterator.next(), gen.getFileSeparator());
            } catch (IndexOutOfBoundsException indexOutOfBoundsException) {
                continue;
            }
            Util.debug("buffered-read (line {}): {}", count, Arrays.toString(rowTokens));
            rows.add(rowTokens);
            if (rows.size() == batch || !iterator.hasNext()) {
                rowGroups.add(rows);
                rows = new ArrayList<>(batch);
                if (rowGroups.size() == batchGroup || !iterator.hasNext()) {
                    queue.put(Either.first(rowGroups));
                    rowGroups = new ArrayList<>(batchGroup);
                }
            }

            if (count % 10_000 == 0) {
                Instant endBatch = Instant.now();
                double rate = Util.calculateRate(10_000, startBatch, endBatch);
                double average = Util.calculateRate(count, startRead, endBatch);
                Util.info("buffered-read {source: {}, progress: {}, rate: {}/s, average: {}/s}",
                        filename, countFormat.format(count), decimalFormat.format(rate), decimalFormat.format(average));
                startBatch = Instant.now();
            }
        }
        queue.put(Either.second(AsyncLoaderWorker.Done.INSTANCE));
        Instant endRead = Instant.now();
        double rate = Util.calculateRate(count, startRead, endRead);
        Util.info("buffered-read {total: {}, rate: {}/s}", countFormat.format(count), decimalFormat.format(rate));
    }

    private CompletableFuture<Void> asyncWrite(int id,
                                               String filename,
                                               Generator gen,
                                               TypeDBSession session,
                                               LinkedBlockingQueue<Either<List<List<String[]>>, AsyncLoaderWorker.Done>> queue) {
        return CompletableFuture.runAsync(() -> {
            Util.debug("async-writer-{} (start): {}", id, filename);
            Either<List<List<String[]>>, Done> queueItem;
            try {
                while ((queueItem = queue.take()).isFirst() && !hasError.get()) {
                    List<List<String[]>> rowGroups = queueItem.first();
                    for (List<String[]> rows : rowGroups) {
                        try (TypeDBTransaction tx = session.transaction(TypeDBTransaction.Type.WRITE)) {
                            rows.forEach(csv -> {
                                appLogger.debug("async-writer-{}: {}", id, csv);
                                try {
                                    gen.write(tx, csv);
                                } catch (Exception e) {
                                    //TODO: here write into error file
                                    e.printStackTrace();
                                }
                            });
                            tx.commit();
                        }
                    }
                }
                assert queueItem.isSecond() || hasError.get();
                if (queueItem.isSecond()) queue.put(queueItem);
            } catch (Throwable e) {
                hasError.set(true);
                appLogger.error("async-writer-" + id + ": " + e.getMessage());
                throw new RuntimeException(e);
            } finally {
                Util.debug("async-writer-{} (end): {}", id, filename);
            }
        }, executor);
    }

    private static class Done {
        private static final Done INSTANCE = new Done();
    }
}
