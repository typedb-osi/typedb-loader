package loader;

import config.Configuration;
import generator.*;

import static util.Util.getSeparator;
import static util.Util.getRowsPerCommit;

import util.TypeDBUtil;
import util.Util;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;

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
    public final ExecutorService executor;
    private final int threads = Runtime.getRuntime().availableProcessors() * 12;
    private final String databaseName;
    private final AtomicBoolean hasError;
    private final int batchGroup;
    private final Configuration dc;

    public AsyncLoaderWorker(Configuration dc, String databaseName) {
        this.dc = dc;
        this.databaseName = databaseName;
        hasError = new AtomicBoolean(false);
        this.batchGroup = 32;
        this.executor = Executors.newFixedThreadPool(threads, new NamedThreadFactory(databaseName));
    }

    public void run(TypeDBClient client) throws IOException, InterruptedException {

        ArrayList<String> orderedBeforeGenerators = dc.getGlobalConfig().getOrderedBeforeGenerators();
        if (orderedBeforeGenerators == null) orderedBeforeGenerators = new ArrayList<>();

        ArrayList<String> orderedAfterGenerators = dc.getGlobalConfig().getOrderedAfterGenerators();
        if (orderedAfterGenerators == null) orderedAfterGenerators = new ArrayList<>();

        ArrayList<String> ignoreGenerators = dc.getGlobalConfig().getIgnoreGenerators();
        if (ignoreGenerators == null) ignoreGenerators = new ArrayList<>();

        try (TypeDBSession session = TypeDBUtil.getDataSession(client, databaseName)) {

            //Load OrderBefore things...
            Util.info("loading ordered before things");
            if (orderedBeforeGenerators.size() > 0) {
                for (String orderedGenerator : orderedBeforeGenerators) {
                    String generatorType = dc.getGeneratorTypeByKey(orderedGenerator);
                    Configuration.Generator generatorConfig = dc.getGeneratorByKey(orderedGenerator);
                    if (ignoreGenerators.stream().noneMatch(orderedGenerator::contains)) {
                        switch (generatorType) {
                            case "attributes":
                                Configuration.Attribute attribute = (Configuration.Attribute) generatorConfig;
                                initializeAttributeConceptValueType(session, attribute);
                                for (String fp : attribute.getDataPaths()) {
                                    Generator gen = new AttributeGenerator(fp, attribute, getSeparator(dc, attribute.getConfig()));
                                    if (!hasError.get())
                                        asyncLoad(session, fp, gen, getRowsPerCommit(dc, attribute.getConfig()));
                                }
                                break;
                            case "entities":
                                Configuration.Entity entity = (Configuration.Entity) generatorConfig;
                                initializeEntityAttributeConceptValueTypes(session, entity);
                                for (String fp : entity.getDataPaths()) {
                                    Generator gen = new EntityGenerator(fp, entity, getSeparator(dc, entity.getConfig()));
                                    if (!hasError.get())
                                        asyncLoad(session, fp, gen, getRowsPerCommit(dc, entity.getConfig()));
                                }
                                break;
                            case "relations":
                                Configuration.Relation relation = (Configuration.Relation) generatorConfig;
                                initializeRelationAttributeConceptValueTypes(session, relation);
                                for (String fp : relation.getDataPaths()) {
                                    Generator gen = new RelationGenerator(fp, relation, getSeparator(dc, relation.getConfig()));
                                    if (!hasError.get())
                                        asyncLoad(session, fp, gen, getRowsPerCommit(dc, relation.getConfig()));
                                }
                                break;
                            case "appendAttribute":
                                Configuration.AppendAttribute appendAttribute = (Configuration.AppendAttribute) generatorConfig;
                                initializeAppendAttributeConceptValueTypes(session, appendAttribute);
                                for (String fp : appendAttribute.getDataPaths()) {
                                    Generator gen = new AppendAttributeGenerator(fp, appendAttribute, getSeparator(dc, appendAttribute.getConfig()));
                                    if (!hasError.get()) {
                                        asyncLoad(session, fp, gen, getRowsPerCommit(dc, appendAttribute.getConfig()));
                                    }
                                }
                                break;
                            case "appendAttributeOrInsertThing":
                                Configuration.AppendAttributeOrInsertThing appendAttributeOrInsertThing = (Configuration.AppendAttributeOrInsertThing) generatorConfig;
                                initializeAppendAttributeConceptValueTypes(session, appendAttributeOrInsertThing);
                                for (String fp : appendAttributeOrInsertThing.getDataPaths()) {
                                    Generator gen = new AppendAttributeOrInsertThingGenerator(fp, appendAttributeOrInsertThing, getSeparator(dc, appendAttributeOrInsertThing.getConfig()));
                                    if (!hasError.get()) {
                                        asyncLoad(session, fp, gen, getRowsPerCommit(dc, appendAttributeOrInsertThing.getConfig()));
                                    }
                                }
                                break;
                        }
                    }
                }
            }


            // Load attributes
            Util.info("loading attributes");
            if (dc.getAttributes() != null) {
                for (Map.Entry<String, Configuration.Attribute> attribute : dc.getAttributes().entrySet()) {
                    if (orderedAfterGenerators.stream().noneMatch(attribute.getKey()::contains) &&
                            orderedBeforeGenerators.stream().noneMatch(attribute.getKey()::contains) &&
                            ignoreGenerators.stream().noneMatch(attribute.getKey()::contains)
                    ) {
                        initializeAttributeConceptValueType(session, attribute.getValue());
                        for (String fp : attribute.getValue().getDataPaths()) {
                            Generator gen = new AttributeGenerator(fp, attribute.getValue(), getSeparator(dc, attribute.getValue().getConfig()));
                            if (!hasError.get())
                                asyncLoad(session, fp, gen, getRowsPerCommit(dc, attribute.getValue().getConfig()));
                        }
                    }
                }
            }

            // Load entities
            Util.info("loading entities");
            if (dc.getEntities() != null) {
                for (Map.Entry<String, Configuration.Entity> entity : dc.getEntities().entrySet()) {
                    if (orderedAfterGenerators.stream().noneMatch(entity.getKey()::contains) &&
                            orderedBeforeGenerators.stream().noneMatch(entity.getKey()::contains) &&
                            ignoreGenerators.stream().noneMatch(entity.getKey()::contains)) {
                        initializeEntityAttributeConceptValueTypes(session, entity.getValue());
                        for (String fp : entity.getValue().getDataPaths()) {
                            Generator gen = new EntityGenerator(fp, entity.getValue(), getSeparator(dc, entity.getValue().getConfig()));
                            if (!hasError.get())
                                asyncLoad(session, fp, gen, getRowsPerCommit(dc, entity.getValue().getConfig()));
                        }
                    }
                }
            }

            //Load relations
            Util.info("loading relations");
            if (dc.getRelations() != null) {
                for (Map.Entry<String, Configuration.Relation> relation : dc.getRelations().entrySet()) {
                    if (orderedAfterGenerators.stream().noneMatch(relation.getKey()::contains) &&
                            orderedBeforeGenerators.stream().noneMatch(relation.getKey()::contains) &&
                            ignoreGenerators.stream().noneMatch(relation.getKey()::contains)) {
                        initializeRelationAttributeConceptValueTypes(session, relation.getValue());
                        for (String fp : relation.getValue().getDataPaths()) {
                            Generator gen = new RelationGenerator(fp, relation.getValue(), getSeparator(dc, relation.getValue().getConfig()));
                            if (!hasError.get()) {
                                asyncLoad(session, fp, gen, getRowsPerCommit(dc, relation.getValue().getConfig()));
                            }
                        }
                    }
                }
            }

            //Load appendAttributes
            Util.info("loading appendAttributes");
            if (dc.getAppendAttribute() != null) {
                for (Map.Entry<String, Configuration.AppendAttribute> appendAttribute : dc.getAppendAttribute().entrySet()) {
                    if (orderedAfterGenerators.stream().noneMatch(appendAttribute.getKey()::contains) &&
                            orderedBeforeGenerators.stream().noneMatch(appendAttribute.getKey()::contains) &&
                            ignoreGenerators.stream().noneMatch(appendAttribute.getKey()::contains)) {
                        initializeAppendAttributeConceptValueTypes(session, appendAttribute.getValue());
                        for (String fp : appendAttribute.getValue().getDataPaths()) {
                            Generator gen = new AppendAttributeGenerator(fp, appendAttribute.getValue(), getSeparator(dc, appendAttribute.getValue().getConfig()));
                            if (!hasError.get()) {
                                asyncLoad(session, fp, gen, getRowsPerCommit(dc, appendAttribute.getValue().getConfig()));
                            }
                        }
                    }
                }
            }

            //Load appendAttributesOrInsertThing
            Util.info("loading appendAttributesOrInsertThing");
            if (dc.getAppendAttributeOrInsertThing() != null) {
                for (Map.Entry<String, Configuration.AppendAttributeOrInsertThing> appendAttributeOrInsertThing : dc.getAppendAttributeOrInsertThing().entrySet()) {
                    if (orderedAfterGenerators.stream().noneMatch(appendAttributeOrInsertThing.getKey()::contains) &&
                            orderedBeforeGenerators.stream().noneMatch(appendAttributeOrInsertThing.getKey()::contains) &&
                            ignoreGenerators.stream().noneMatch(appendAttributeOrInsertThing.getKey()::contains)) {
                        initializeAppendAttributeConceptValueTypes(session, appendAttributeOrInsertThing.getValue());
                        for (String fp : appendAttributeOrInsertThing.getValue().getDataPaths()) {
                            Generator gen = new AppendAttributeOrInsertThingGenerator(fp, appendAttributeOrInsertThing.getValue(), getSeparator(dc, appendAttributeOrInsertThing.getValue().getConfig()));
                            if (!hasError.get()) {
                                asyncLoad(session, fp, gen, getRowsPerCommit(dc, appendAttributeOrInsertThing.getValue().getConfig()));
                            }
                        }
                    }
                }
            }

            //Load OrderAfter things...
            Util.info("loading ordered after things");
            if (orderedAfterGenerators.size() > 0) {
                for (String orderedGenerator : orderedAfterGenerators) {
                    String generatorType = dc.getGeneratorTypeByKey(orderedGenerator);
                    Configuration.Generator generatorConfig = dc.getGeneratorByKey(orderedGenerator);
                    if (ignoreGenerators.stream().noneMatch(orderedGenerator::contains)) {
                        switch (generatorType) {
                            case "attributes":
                                Configuration.Attribute attribute = (Configuration.Attribute) generatorConfig;
                                initializeAttributeConceptValueType(session, attribute);
                                for (String fp : attribute.getDataPaths()) {
                                    Generator gen = new AttributeGenerator(fp, attribute, getSeparator(dc, attribute.getConfig()));
                                    if (!hasError.get())
                                        asyncLoad(session, fp, gen, getRowsPerCommit(dc, attribute.getConfig()));
                                }
                                break;
                            case "entities":
                                Configuration.Entity entity = (Configuration.Entity) generatorConfig;
                                initializeEntityAttributeConceptValueTypes(session, entity);
                                for (String fp : entity.getDataPaths()) {
                                    Generator gen = new EntityGenerator(fp, entity, getSeparator(dc, entity.getConfig()));
                                    if (!hasError.get())
                                        asyncLoad(session, fp, gen, getRowsPerCommit(dc, entity.getConfig()));
                                }
                                break;
                            case "relations":
                                Configuration.Relation relation = (Configuration.Relation) generatorConfig;
                                initializeRelationAttributeConceptValueTypes(session, relation);
                                for (String fp : relation.getDataPaths()) {
                                    Generator gen = new RelationGenerator(fp, relation, getSeparator(dc, relation.getConfig()));
                                    if (!hasError.get())
                                        asyncLoad(session, fp, gen, getRowsPerCommit(dc, relation.getConfig()));
                                }
                                break;
                            case "appendAttribute":
                                Configuration.AppendAttribute appendAttribute = (Configuration.AppendAttribute) generatorConfig;
                                initializeAppendAttributeConceptValueTypes(session, appendAttribute);
                                for (String fp : appendAttribute.getDataPaths()) {
                                    Generator gen = new AppendAttributeGenerator(fp, appendAttribute, getSeparator(dc, appendAttribute.getConfig()));
                                    if (!hasError.get()) {
                                        asyncLoad(session, fp, gen, getRowsPerCommit(dc, appendAttribute.getConfig()));
                                    }
                                }
                                break;
                            case "appendAttributeOrInsertThing":
                                Configuration.AppendAttributeOrInsertThing appendAttributeOrInsertThing = (Configuration.AppendAttributeOrInsertThing) generatorConfig;
                                initializeAppendAttributeConceptValueTypes(session, appendAttributeOrInsertThing);
                                for (String fp : appendAttributeOrInsertThing.getDataPaths()) {
                                    Generator gen = new AppendAttributeOrInsertThingGenerator(fp, appendAttributeOrInsertThing, getSeparator(dc, appendAttributeOrInsertThing.getConfig()));
                                    if (!hasError.get()) {
                                        asyncLoad(session, fp, gen, getRowsPerCommit(dc, appendAttributeOrInsertThing.getConfig()));
                                    }
                                }
                                break;
                        }
                    }
                }
            }

            //Finished
            Util.info("TypeDB Loader finished");
        }
    }

    private void initializeAppendAttributeConceptValueTypes(TypeDBSession session, Configuration.AppendAttribute appendAttribute) {
        Configuration.ConstrainingAttribute[] hasAttributes = appendAttribute.getAttributes();
        if (hasAttributes != null) {
            Util.setConstrainingAttributeConceptType(hasAttributes, session);
        }
        Configuration.ThingGetter thingGetter = appendAttribute.getThingGetter();
        if (thingGetter != null) {
            Configuration.ConstrainingAttribute[] matchAttributes = appendAttribute.getThingGetter().getThingGetters();
            if (matchAttributes != null) {
                Util.setConstrainingAttributeConceptType(matchAttributes, session);
            }
        }
    }

    private void initializeAttributeConceptValueType(TypeDBSession session, Configuration.Attribute attribute) {
        Configuration.ConstrainingAttribute[] attributes = new Configuration.ConstrainingAttribute[1];
        attributes[0] = attribute.getAttribute();
        Util.setConstrainingAttributeConceptType(attributes, session);
    }

    private void initializeEntityAttributeConceptValueTypes(TypeDBSession session, Configuration.Entity entity) {
        Configuration.ConstrainingAttribute[] hasAttributes = entity.getAttributes();
        Util.setConstrainingAttributeConceptType(hasAttributes, session);
    }

    private void initializeRelationAttributeConceptValueTypes(TypeDBSession session, Configuration.Relation relation) {
        Configuration.ConstrainingAttribute[] hasAttributes = relation.getAttributes();
        if (hasAttributes != null) {
            Util.setConstrainingAttributeConceptType(hasAttributes, session);
        }
        for (int idx = 0; idx < relation.getPlayers().length; idx++) {
            Util.setGetterAttributeConceptType(relation, idx, session);
        }
    }

    private void asyncLoad(TypeDBSession session,
                           String filename,
                           Generator gen,
                           int batch) throws IOException, InterruptedException {
        Util.info("async-load (start): {}", filename);
        LinkedBlockingQueue<Either<List<List<String[]>>, Done>> queue = new LinkedBlockingQueue<>(threads * 2);
        List<CompletableFuture<Void>> asyncWrites = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            asyncWrites.add(asyncWrite(i + 1, filename, gen, session, queue));
        }
        bufferedRead(filename, gen, batch, queue);
        CompletableFuture.allOf(asyncWrites.toArray(new CompletableFuture[0])).join();
        Util.info("async-load (end): {}", filename);
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
            Util.debug("buffered-read: (line {}): {}", count, Arrays.toString(rowTokens));
            rows.add(rowTokens);
            if (rows.size() == batch || !iterator.hasNext()) {
                rowGroups.add(rows);
                rows = new ArrayList<>(batch);
                if (rowGroups.size() == batchGroup || !iterator.hasNext()) {
                    queue.put(Either.first(rowGroups));
                    rowGroups = new ArrayList<>(batchGroup);
                }
            }

            if (count % 50_000 == 0) {
                Instant endBatch = Instant.now();
                double rate = Util.calculateRate(50_000, startBatch, endBatch);
                double average = Util.calculateRate(count, startRead, endBatch);
                Util.info("buffered-read: source: {}, progress: {}, rate: {}/s, average: {}/s",
                        filename, countFormat.format(count), decimalFormat.format(rate), decimalFormat.format(average));
                startBatch = Instant.now();
            }
        }
        queue.put(Either.second(AsyncLoaderWorker.Done.INSTANCE));
        Instant endRead = Instant.now();
        double rate = Util.calculateRate(count, startRead, endRead);
        Util.info("buffered-read: total: {}, rate: {}/s", countFormat.format(count), decimalFormat.format(rate));
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
                                Util.debug("async-writer-{}: {}", id, csv);
                                gen.write(tx, csv);
                            });
                            tx.commit();
                        }
                    }
                }
                assert queueItem.isSecond() || hasError.get();
                if (queueItem.isSecond()) queue.put(queueItem);
            } catch (Throwable e) {
                hasError.set(true);
                Util.error("async-writer-" + id + ": " + e.getMessage());
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
