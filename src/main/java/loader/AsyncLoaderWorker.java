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

package loader;

import config.Configuration;
import generator.*;

import util.TypeDBUtil;
import util.Util;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static util.Util.*;

public class AsyncLoaderWorker {

    private static final DecimalFormat countFormat = new DecimalFormat("#,###");
    private static final DecimalFormat decimalFormat = new DecimalFormat("#,###.00");
    final ExecutorService executor;
    private final int threads;
    private final String databaseName;
    private final AtomicBoolean hasError;
    private final int batchGroup;
    private final Configuration dc;

    public AsyncLoaderWorker(Configuration dc, String databaseName) {
        this.dc = dc;
        this.threads = dc.getGlobalConfig().getParallelisation();
        this.databaseName = databaseName;
        this.hasError = new AtomicBoolean(false);
        this.batchGroup = 1;
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
                    if (ignoreGenerators.stream().noneMatch(orderedGenerator::equals)) {
                        switch (generatorType) {
                            case "attributes":
                                Configuration.Attribute attribute = (Configuration.Attribute) generatorConfig;
                                initializeAttributeConceptValueType(session, attribute.getInsert());
                                for (String fp : attribute.getData()) {
                                    Generator gen = new AttributeGenerator(fp, attribute, getSeparator(dc, attribute.getConfig()));
                                    if (!hasError.get())
                                        asyncLoad(session, orderedGenerator, fp, gen, getRowsPerCommit(dc, attribute.getConfig()));
                                }
                                break;
                            case "entities":
                                Configuration.Entity entity = (Configuration.Entity) generatorConfig;
                                Util.setConstrainingAttributeConceptType(entity.getInsert().getOwnerships(), session);
                                for (String fp : entity.getData()) {
                                    Generator gen = new EntityGenerator(fp, entity, getSeparator(dc, entity.getConfig()));
                                    if (!hasError.get())
                                        asyncLoad(session, orderedGenerator, fp, gen, getRowsPerCommit(dc, entity.getConfig()));
                                }
                                break;
                            case "relations":
                                Configuration.Relation relation = (Configuration.Relation) generatorConfig;
                                initializeRelationAttributeConceptValueTypes(session, relation);
                                for (String fp : relation.getData()) {
                                    Generator gen = new RelationGenerator(fp, relation, getSeparator(dc, relation.getConfig()));
                                    if (!hasError.get())
                                        asyncLoad(session, orderedGenerator, fp, gen, getRowsPerCommit(dc, relation.getConfig()));
                                }
                                break;
                            case "appendAttribute":
                                Configuration.AppendAttribute appendAttribute = (Configuration.AppendAttribute) generatorConfig;
                                initializeAppendAttributeConceptValueTypes(session, appendAttribute);
                                for (String fp : appendAttribute.getData()) {
                                    Generator gen = new AppendAttributeGenerator(fp, appendAttribute, getSeparator(dc, appendAttribute.getConfig()));
                                    if (!hasError.get()) {
                                        asyncLoad(session, orderedGenerator, fp, gen, getRowsPerCommit(dc, appendAttribute.getConfig()));
                                    }
                                }
                                break;
                            case "appendAttributeOrInsertThing":
                                Configuration.AppendAttributeOrInsertThing appendAttributeOrInsertThing = (Configuration.AppendAttributeOrInsertThing) generatorConfig;
                                initializeAppendAttributeConceptValueTypes(session, appendAttributeOrInsertThing);
                                for (String fp : appendAttributeOrInsertThing.getData()) {
                                    Generator gen = new AppendAttributeOrInsertThingGenerator(fp, appendAttributeOrInsertThing, getSeparator(dc, appendAttributeOrInsertThing.getConfig()));
                                    if (!hasError.get()) {
                                        asyncLoad(session, orderedGenerator, fp, gen, getRowsPerCommit(dc, appendAttributeOrInsertThing.getConfig()));
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
                    if (orderedAfterGenerators.stream().noneMatch(attribute.getKey()::equals) &&
                            orderedBeforeGenerators.stream().noneMatch(attribute.getKey()::equals) &&
                            ignoreGenerators.stream().noneMatch(attribute.getKey()::equals)
                    ) {
                        initializeAttributeConceptValueType(session, attribute.getValue().getInsert());
                        for (String fp : attribute.getValue().getData()) {
                            Generator gen = new AttributeGenerator(fp, attribute.getValue(), getSeparator(dc, attribute.getValue().getConfig()));
                            if (!hasError.get())
                                asyncLoad(session, attribute.getKey(), fp, gen, getRowsPerCommit(dc, attribute.getValue().getConfig()));
                        }
                    }
                }
            }

            // Load entities
            Util.info("loading entities");
            if (dc.getEntities() != null) {
                for (Map.Entry<String, Configuration.Entity> entity : dc.getEntities().entrySet()) {
                    if (orderedAfterGenerators.stream().noneMatch(entity.getKey()::equals) &&
                            orderedBeforeGenerators.stream().noneMatch(entity.getKey()::equals) &&
                            ignoreGenerators.stream().noneMatch(entity.getKey()::equals)) {
                        System.out.println(entity.getKey());
                        Util.setConstrainingAttributeConceptType(entity.getValue().getInsert().getOwnerships(), session);
                        for (String fp : entity.getValue().getData()) {
                            Generator gen = new EntityGenerator(fp, entity.getValue(), getSeparator(dc, entity.getValue().getConfig()));
                            if (!hasError.get())
                                asyncLoad(session, entity.getKey(), fp, gen, getRowsPerCommit(dc, entity.getValue().getConfig()));
                        }
                    }
                }
            }

            //Load relations
            Util.info("loading relations");
            if (dc.getRelations() != null) {
                for (Map.Entry<String, Configuration.Relation> relation : dc.getRelations().entrySet()) {
                    if (orderedAfterGenerators.stream().noneMatch(relation.getKey()::equals) &&
                            orderedBeforeGenerators.stream().noneMatch(relation.getKey()::equals) &&
                            ignoreGenerators.stream().noneMatch(relation.getKey()::equals)) {
                        initializeRelationAttributeConceptValueTypes(session, relation.getValue());
                        for (String fp : relation.getValue().getData()) {
                            Generator gen = new RelationGenerator(fp, relation.getValue(), getSeparator(dc, relation.getValue().getConfig()));
                            if (!hasError.get()) {
                                asyncLoad(session, relation.getKey(), fp, gen, getRowsPerCommit(dc, relation.getValue().getConfig()));
                            }
                        }
                    }
                }
            }

            //Load appendAttributes
            Util.info("loading appendAttributes");
            if (dc.getAppendAttribute() != null) {
                for (Map.Entry<String, Configuration.AppendAttribute> appendAttribute : dc.getAppendAttribute().entrySet()) {
                    if (orderedAfterGenerators.stream().noneMatch(appendAttribute.getKey()::equals) &&
                            orderedBeforeGenerators.stream().noneMatch(appendAttribute.getKey()::equals) &&
                            ignoreGenerators.stream().noneMatch(appendAttribute.getKey()::equals)) {
                        initializeAppendAttributeConceptValueTypes(session, appendAttribute.getValue());
                        for (String fp : appendAttribute.getValue().getData()) {
                            Generator gen = new AppendAttributeGenerator(fp, appendAttribute.getValue(), getSeparator(dc, appendAttribute.getValue().getConfig()));
                            if (!hasError.get()) {
                                asyncLoad(session, appendAttribute.getKey(), fp, gen, getRowsPerCommit(dc, appendAttribute.getValue().getConfig()));
                            }
                        }
                    }
                }
            }

            //Load appendAttributesOrInsertThing
            Util.info("loading appendAttributesOrInsertThing");
            if (dc.getAppendAttributeOrInsertThing() != null) {
                for (Map.Entry<String, Configuration.AppendAttributeOrInsertThing> appendAttributeOrInsertThing : dc.getAppendAttributeOrInsertThing().entrySet()) {
                    if (orderedAfterGenerators.stream().noneMatch(appendAttributeOrInsertThing.getKey()::equals) &&
                            orderedBeforeGenerators.stream().noneMatch(appendAttributeOrInsertThing.getKey()::equals) &&
                            ignoreGenerators.stream().noneMatch(appendAttributeOrInsertThing.getKey()::equals)) {
                        initializeAppendAttributeConceptValueTypes(session, appendAttributeOrInsertThing.getValue());
                        for (String fp : appendAttributeOrInsertThing.getValue().getData()) {
                            Generator gen = new AppendAttributeOrInsertThingGenerator(fp, appendAttributeOrInsertThing.getValue(), getSeparator(dc, appendAttributeOrInsertThing.getValue().getConfig()));
                            if (!hasError.get()) {
                                asyncLoad(session, appendAttributeOrInsertThing.getKey(), fp, gen, getRowsPerCommit(dc, appendAttributeOrInsertThing.getValue().getConfig()));
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
                    if (ignoreGenerators.stream().noneMatch(orderedGenerator::equals)) {
                        switch (generatorType) {
                            case "attributes":
                                Configuration.Attribute attribute = (Configuration.Attribute) generatorConfig;
                                initializeAttributeConceptValueType(session, attribute.getInsert());
                                for (String fp : attribute.getData()) {
                                    Generator gen = new AttributeGenerator(fp, attribute, getSeparator(dc, attribute.getConfig()));
                                    if (!hasError.get())
                                        asyncLoad(session, orderedGenerator, fp, gen, getRowsPerCommit(dc, attribute.getConfig()));
                                }
                                break;
                            case "entities":
                                Configuration.Entity entity = (Configuration.Entity) generatorConfig;
                                Util.setConstrainingAttributeConceptType(entity.getInsert().getOwnerships(), session);
                                for (String fp : entity.getData()) {
                                    Generator gen = new EntityGenerator(fp, entity, getSeparator(dc, entity.getConfig()));
                                    if (!hasError.get())
                                        asyncLoad(session, orderedGenerator, fp, gen, getRowsPerCommit(dc, entity.getConfig()));
                                }
                                break;
                            case "relations":
                                Configuration.Relation relation = (Configuration.Relation) generatorConfig;
                                initializeRelationAttributeConceptValueTypes(session, relation);
                                for (String fp : relation.getData()) {
                                    Generator gen = new RelationGenerator(fp, relation, getSeparator(dc, relation.getConfig()));
                                    if (!hasError.get())
                                        asyncLoad(session, orderedGenerator, fp, gen, getRowsPerCommit(dc, relation.getConfig()));
                                }
                                break;
                            case "appendAttribute":
                                Configuration.AppendAttribute appendAttribute = (Configuration.AppendAttribute) generatorConfig;
                                initializeAppendAttributeConceptValueTypes(session, appendAttribute);
                                for (String fp : appendAttribute.getData()) {
                                    Generator gen = new AppendAttributeGenerator(fp, appendAttribute, getSeparator(dc, appendAttribute.getConfig()));
                                    if (!hasError.get()) {
                                        asyncLoad(session, orderedGenerator, fp, gen, getRowsPerCommit(dc, appendAttribute.getConfig()));
                                    }
                                }
                                break;
                            case "appendAttributeOrInsertThing":
                                Configuration.AppendAttributeOrInsertThing appendAttributeOrInsertThing = (Configuration.AppendAttributeOrInsertThing) generatorConfig;
                                initializeAppendAttributeConceptValueTypes(session, appendAttributeOrInsertThing);
                                for (String fp : appendAttributeOrInsertThing.getData()) {
                                    Generator gen = new AppendAttributeOrInsertThingGenerator(fp, appendAttributeOrInsertThing, getSeparator(dc, appendAttributeOrInsertThing.getConfig()));
                                    if (!hasError.get()) {
                                        asyncLoad(session, orderedGenerator, fp, gen, getRowsPerCommit(dc, appendAttributeOrInsertThing.getConfig()));
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
        Configuration.ConstrainingAttribute[] hasAttributes = appendAttribute.getInsert().getOwnerships();
        if (hasAttributes != null) {
            Util.setConstrainingAttributeConceptType(hasAttributes, session);
        }

        Configuration.ConstrainingAttribute[] matchAttributes = appendAttribute.getMatch().getOwnerships();
        if (matchAttributes != null) {
            Util.setConstrainingAttributeConceptType(matchAttributes, session);
        }
    }

    private void initializeAttributeConceptValueType(TypeDBSession session, Configuration.ConstrainingAttribute attribute) {
        Configuration.ConstrainingAttribute[] attributes = new Configuration.ConstrainingAttribute[1];
        attributes[0] = attribute;
        Util.setConstrainingAttributeConceptType(attributes, session);
    }

    private void initializeRelationAttributeConceptValueTypes(TypeDBSession session, Configuration.Relation relation) {
        Configuration.ConstrainingAttribute[] hasAttributes = relation.getInsert().getOwnerships();
        if (hasAttributes != null) {
            Util.setConstrainingAttributeConceptType(hasAttributes, session);
        }
        for (Configuration.Player player: relation.getInsert().getPlayers()) {
            recursiveSetPlayerAttributeConceptValueTypes(session, player);
        }
    }

    private void recursiveSetPlayerAttributeConceptValueTypes(TypeDBSession session, Configuration.Player player) {
        if (playerType(player).equals("attribute")) {
            //terminating condition - attribute player:
            Configuration.ConstrainingAttribute currentAttribute = player.getMatch().getAttribute();
            currentAttribute.setAttribute(player.getMatch().getType());
            initializeAttributeConceptValueType(session, currentAttribute);
        } else if (playerType(player).equals("byAttribute")) {
            //terminating condition - byAttribute player:
            Util.setConstrainingAttributeConceptType(player.getMatch().getOwnerships(), session);
        } else if (playerType(player).equals("byPlayer")) {
            for (Configuration.Player curPlayer: player.getMatch().getPlayers()) {
                recursiveSetPlayerAttributeConceptValueTypes(session, curPlayer);
            }
        }
    }

    private void asyncLoad(TypeDBSession session,
                           String generatorKey,
                           String filename,
                           Generator gen,
                           int batch) throws IOException, InterruptedException {
        Util.info("async-load (start): {} reading from {}", generatorKey, filename);
        LinkedBlockingQueue<Either<List<List<String[]>>, Done>> queue = new LinkedBlockingQueue<>(threads * 4);
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

        BufferedReader br = Util.newBufferedReader(filename);

        Iterator<String> iterator = br.lines().skip(1).iterator();
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
