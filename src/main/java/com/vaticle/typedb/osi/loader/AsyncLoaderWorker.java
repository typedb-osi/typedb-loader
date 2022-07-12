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

package com.vaticle.typedb.osi.loader;

import com.vaticle.typedb.client.api.TypeDBClient;
import com.vaticle.typedb.client.api.TypeDBSession;
import com.vaticle.typedb.client.api.TypeDBTransaction;
import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.osi.config.Configuration;
import com.vaticle.typedb.osi.generator.*;
import com.vaticle.typedb.osi.util.TypeDBUtil;
import com.vaticle.typedb.osi.util.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.vaticle.typedb.osi.util.Util.getRowsPerCommit;
import static com.vaticle.typedb.osi.util.Util.getSeparator;
import static com.vaticle.typedb.osi.util.Util.playerType;

public class AsyncLoaderWorker {

    private static final DecimalFormat countFormat = new DecimalFormat("#,###");
    private static final DecimalFormat decimalFormat = new DecimalFormat("#,###.00");
    private final ExecutorService executor;
    private final int threads;
    private final String databaseName;
    private final AtomicBoolean hasError;
    private final int batchGroup;
    private final Configuration dc;
    private Status status;

    private enum Status {OK, ERROR}

    public AsyncLoaderWorker(Configuration dc, String databaseName) {
        this.dc = dc;
        this.threads = dc.getGlobalConfig().getParallelisation();
        this.databaseName = databaseName;
        this.hasError = new AtomicBoolean(false);
        this.batchGroup = 1;
        this.executor = Executors.newFixedThreadPool(threads, new NamedThreadFactory(databaseName));
        this.status = Status.OK;
    }

    public void run(TypeDBClient client) throws IOException, InterruptedException {

        ArrayList<String> orderedBeforeGenerators = dc.getGlobalConfig().getOrderedBeforeGenerators();
        if (orderedBeforeGenerators == null) orderedBeforeGenerators = new ArrayList<>();

        ArrayList<String> orderedAfterGenerators = dc.getGlobalConfig().getOrderedAfterGenerators();
        if (orderedAfterGenerators == null) orderedAfterGenerators = new ArrayList<>();

        ArrayList<String> ignoreGenerators = dc.getGlobalConfig().getIgnoreGenerators();
        if (ignoreGenerators == null) ignoreGenerators = new ArrayList<>();

        Set<String> separateGenerators = new HashSet<>();
        separateGenerators.addAll(orderedBeforeGenerators);
        separateGenerators.addAll(orderedAfterGenerators);
        separateGenerators.addAll(ignoreGenerators);

        try (TypeDBSession session = TypeDBUtil.getDataSession(client, databaseName)) {

            //Load OrderBefore things...
            Util.info("loading ordered before things");
            for (String generatorKey : orderedBeforeGenerators) {
                if (!ignoreGenerators.contains(generatorKey)) {
                    executeGenerator(
                            session,
                            generatorKey,
                            dc.getGeneratorTypeByKey(generatorKey),
                            dc.getGeneratorByKey(generatorKey)
                    );
                    if (status == Status.ERROR) return;
                }
            }

            // Load attributes
            Util.info("loading attributes");
            if (dc.getAttributes() != null) {
                for (Map.Entry<String, Configuration.Generator.Attribute> attribute : dc.getAttributes().entrySet()) {
                    if (!separateGenerators.contains(attribute.getKey())) {
                        loadAttribute(session, attribute.getKey(), attribute.getValue());
                        if (status == Status.ERROR) return;
                    }
                }
            }

            // Load entities
            Util.info("loading entities");
            if (dc.getEntities() != null) {
                for (Map.Entry<String, Configuration.Generator.Entity> entity : dc.getEntities().entrySet()) {
                    if (!separateGenerators.contains(entity.getKey())) {
                        loadEntity(session, entity.getKey(), entity.getValue());
                        if (status == Status.ERROR) return;
                    }
                }
            }

            //Load relations
            Util.info("loading relations");
            if (dc.getRelations() != null) {
                for (Map.Entry<String, Configuration.Generator.Relation> relation : dc.getRelations().entrySet()) {
                    if (!separateGenerators.contains(relation.getKey())) {
                        loadRelation(session, relation.getKey(), relation.getValue());
                        if (status == Status.ERROR) return;
                    }
                }
            }

            //Load appendAttributes
            Util.info("loading appendAttributes");
            if (dc.getAppendAttribute() != null) {
                for (Map.Entry<String, Configuration.Generator.AppendAttribute> appendAttribute : dc.getAppendAttribute().entrySet()) {
                    if (!separateGenerators.contains(appendAttribute.getKey())) {
                        loadAppendAttribute(session, appendAttribute.getKey(), appendAttribute.getValue());
                        if (status == Status.ERROR) return;
                    }
                }
            }

            //Load appendAttributesOrInsertThing
            Util.info("loading appendAttributesOrInsertThing");
            if (dc.getAppendAttributeOrInsertThing() != null) {
                for (Map.Entry<String, Configuration.Generator.AppendAttributeOrInsertThing> appendAttributeOrInsertThing : dc.getAppendAttributeOrInsertThing().entrySet()) {
                    if (!separateGenerators.contains(appendAttributeOrInsertThing.getKey())) {
                        loadAppendOrInsert(session, appendAttributeOrInsertThing.getKey(), appendAttributeOrInsertThing.getValue());
                        if (status == Status.ERROR) return;
                    }
                }
            }

            //Load OrderAfter things...
            Util.info("loading ordered after things");
            if (orderedAfterGenerators.size() > 0) {
                for (String orderedGenerator : orderedAfterGenerators) {
                    if (!ignoreGenerators.contains(orderedGenerator)) {
                        executeGenerator(
                                session,
                                orderedGenerator,
                                dc.getGeneratorTypeByKey(orderedGenerator),
                                dc.getGeneratorByKey(orderedGenerator)
                        );
                        if (status == Status.ERROR) return;
                    }
                }
            }

            //Finished
            Util.info("TypeDB Loader finished");
        }
    }

    public void close() {
        executor.shutdown();
    }

    private void loadAttribute(TypeDBSession session, String generatorKey, Configuration.Generator.Attribute attributeGenerator)
            throws IOException, InterruptedException {
        initializeAttributeConceptValueType(session, attributeGenerator.getInsert());
        for (String filePath : attributeGenerator.getData()) {
            Generator gen = new AttributeGenerator(filePath, attributeGenerator, getSeparator(dc, attributeGenerator.getConfig()));
            asyncLoad(session, generatorKey, filePath, gen, getRowsPerCommit(dc, attributeGenerator.getConfig()));
            if (status == Status.ERROR) return;
        }
    }

    private void loadEntity(TypeDBSession session, String generatorKey, Configuration.Generator.Entity entityGenerator)
            throws IOException, InterruptedException {
        Util.setConstrainingAttributeConceptType(entityGenerator.getInsert().getOwnerships(), session);
        for (String filePath : entityGenerator.getData()) {
            Generator gen = new EntityGenerator(filePath, entityGenerator, getSeparator(dc, entityGenerator.getConfig()));
            asyncLoad(session, generatorKey, filePath, gen, getRowsPerCommit(dc, entityGenerator.getConfig()));
            if (status == Status.ERROR) return;
        }
    }

    private void loadRelation(TypeDBSession session, String generatorKey, Configuration.Generator.Relation relation)
            throws IOException, InterruptedException {
        initializeRelationAttributeConceptValueTypes(session, relation);
        for (String filePath : relation.getData()) {
            Generator gen = new RelationGenerator(filePath, relation, getSeparator(dc, relation.getConfig()));
            asyncLoad(session, generatorKey, filePath, gen, getRowsPerCommit(dc, relation.getConfig()));
            if (status == Status.ERROR) return;
        }
    }

    private void loadAppendAttribute(TypeDBSession session, String generatorKey, Configuration.Generator.AppendAttribute appendAttribute)
            throws IOException, InterruptedException {
        initializeAppendAttributeConceptValueTypes(session, appendAttribute);
        for (String filePath : appendAttribute.getData()) {
            Generator gen = new AppendAttributeGenerator(filePath, appendAttribute, getSeparator(dc, appendAttribute.getConfig()));
            asyncLoad(session, generatorKey, filePath, gen, getRowsPerCommit(dc, appendAttribute.getConfig()));
            if (status == Status.ERROR) return;
        }
    }

    private void loadAppendOrInsert(TypeDBSession session, String generatorKey,
                                      Configuration.Generator.AppendAttributeOrInsertThing appendAttributeOrInsertThing)
            throws IOException, InterruptedException {
        initializeAppendAttributeConceptValueTypes(session, appendAttributeOrInsertThing);
        for (String filePath : appendAttributeOrInsertThing.getData()) {
            Generator gen = new AppendAttributeOrInsertThingGenerator(filePath, appendAttributeOrInsertThing, getSeparator(dc, appendAttributeOrInsertThing.getConfig()));
            asyncLoad(session, generatorKey, filePath, gen, getRowsPerCommit(dc, appendAttributeOrInsertThing.getConfig()));
            if (status == Status.ERROR) return;
        }
    }

    private void executeGenerator(TypeDBSession session, String generatorKey, String generatorType,
                                    Configuration.Generator generatorConfig) throws IOException, InterruptedException {
        switch (generatorType) {
            case "attributes":
                loadAttribute(session, generatorKey, (Configuration.Generator.Attribute) generatorConfig);
                break;
            case "entities":
                 loadEntity(session, generatorKey, (Configuration.Generator.Entity) generatorConfig);
                 break;
            case "relations":
                 loadRelation(session, generatorKey, (Configuration.Generator.Relation) generatorConfig);
                 break;
            case "appendAttribute":
                 loadAppendAttribute(session, generatorKey, (Configuration.Generator.AppendAttribute) generatorConfig);
                 break;
            case "appendAttributeOrInsertThing":
                 loadAppendOrInsert(session, generatorKey, (Configuration.Generator.AppendAttributeOrInsertThing) generatorConfig);
                 break;
            default:
                throw new RuntimeException("Unrecognised generator type: " + generatorType);
        }
    }

    private void initializeAppendAttributeConceptValueTypes(TypeDBSession session, Configuration.Generator.AppendAttribute appendAttribute) {
        Configuration.Definition.Attribute[] hasAttributes = appendAttribute.getInsert().getOwnerships();
        if (hasAttributes != null) {
            Util.setConstrainingAttributeConceptType(hasAttributes, session);
        }

        Configuration.Definition.Attribute[] matchAttributes = appendAttribute.getMatch().getOwnerships();
        if (matchAttributes != null) {
            Util.setConstrainingAttributeConceptType(matchAttributes, session);
        }
    }

    private void initializeAttributeConceptValueType(TypeDBSession session, Configuration.Definition.Attribute attribute) {
        Configuration.Definition.Attribute[] attributes = new Configuration.Definition.Attribute[1];
        attributes[0] = attribute;
        Util.setConstrainingAttributeConceptType(attributes, session);
    }

    private void initializeRelationAttributeConceptValueTypes(TypeDBSession session, Configuration.Generator.Relation relation) {
        Configuration.Definition.Attribute[] hasAttributes = relation.getInsert().getOwnerships();
        if (hasAttributes != null) {
            Util.setConstrainingAttributeConceptType(hasAttributes, session);
        }
        for (Configuration.Definition.Player player : relation.getInsert().getPlayers()) {
            recursiveSetPlayerAttributeConceptValueTypes(session, player);
        }
    }

    private void recursiveSetPlayerAttributeConceptValueTypes(TypeDBSession session, Configuration.Definition.Player player) {
        if (playerType(player).equals("attribute")) {
            //terminating condition - attribute player:
            Configuration.Definition.Attribute currentAttribute = player.getMatch().getAttribute();
            currentAttribute.setAttribute(player.getMatch().getType());
            initializeAttributeConceptValueType(session, currentAttribute);
        } else if (playerType(player).equals("byAttribute")) {
            //terminating condition - byAttribute player:
            Util.setConstrainingAttributeConceptType(player.getMatch().getOwnerships(), session);
        } else if (playerType(player).equals("byPlayer")) {
            for (Configuration.Definition.Player curPlayer : player.getMatch().getPlayers()) {
                recursiveSetPlayerAttributeConceptValueTypes(session, curPlayer);
            }
        }
    }

    private void asyncLoad(TypeDBSession session, String generatorKey, String filename, Generator gen, int batch)
            throws IOException, InterruptedException {
        Util.info("async-load (start): {} reading from {}", generatorKey, filename);
        LinkedBlockingQueue<Either<List<List<String[]>>, Done>> queue = new LinkedBlockingQueue<>(threads * 4);
        List<CompletableFuture<Void>> asyncWrites = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            asyncWrites.add(asyncWrite(i + 1, filename, gen, session, queue));
        }
        bufferedRead(filename, gen, batch, queue);
        CompletableFuture.allOf(asyncWrites.toArray(new CompletableFuture[0])).join();
        Util.info("async-load (end): {}", filename);
        if (hasError.get()) status = Status.ERROR;
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
