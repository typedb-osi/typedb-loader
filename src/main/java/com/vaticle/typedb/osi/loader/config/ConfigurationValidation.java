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

package com.vaticle.typedb.osi.loader.config;

import com.vaticle.typedb.client.api.TypeDBSession;
import com.vaticle.typedb.client.api.TypeDBTransaction;
import com.vaticle.typedb.client.api.answer.ConceptMap;
import com.vaticle.typedb.client.common.exception.TypeDBClientException;
import com.vaticle.typedb.osi.loader.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class ConfigurationValidation {

    private final Configuration configuration;

    public ConfigurationValidation(Configuration configuration) {
        this.configuration = configuration;
    }

    public void validateConfiguration(HashMap<String, ArrayList<String>> validationReport,
                                      TypeDBSession session) {

        // DEFAULT CONFIG (note: schema will be validated beforehand in TypeDBLoader.java because need migrated schema for validation that follows):
        Configuration.GlobalConfig globalConfig = configuration.getGlobalConfig();
        if (globalConfig != null) {
            //ROWS PER COMMIT
            if (globalConfig.getRowsPerCommit() != null) {
                if (globalConfig.getRowsPerCommit() > 150) {
                    validationReport.get("warnings").add("defaultConfig.rowsPerCommit is set to be > 150 - in most cases, choosing a value between 50 and 150 gives the best performance");
                }
            } else {
                validationReport.get("warnings").add("defaultConfig.rowsPerCommit is not set - it must therefore be set individually for each generator");
            }
            //SEPARATOR
            if (globalConfig.getSeparator() == null) {
                validationReport.get("warnings").add("defaultConfig.separator is not set - it must therefore be set individually for each generator");
            }
            //PARALLELISATION
            if (globalConfig.getParallelisation() != null) {
                validationReport.get("warnings").add("defaultConfig.parallelisation is not set - defaults to number of processors on machine * 8");
            }
            //IGNORE_GENERATORS
            if (globalConfig.getIgnoreGenerators() != null) {
                validationReport.get("warnings").add("defaultConfig.ignoreGenerators: ignoring generators: [" + String.join(", ", globalConfig.getIgnoreGenerators()) + "]");
            }
            //ORDERED_BEFORE_GENERATORS
            if (globalConfig.getOrderedBeforeGenerators() != null) {
                validationReport.get("warnings").add("defaultConfig.orderedBeforeGenerators: running generators: [" + String.join(", ", globalConfig.getIgnoreGenerators()) + "] before rest of config in order.");
            }
            //ORDERED_AFTER_GENERATORS
            if (globalConfig.getOrderedAfterGenerators() != null) {
                validationReport.get("warnings").add("defaultConfig.orderedAfterGenerators: running generators: [" + String.join(", ", globalConfig.getIgnoreGenerators()) + "] before rest of config in order.");
            }
        } else {
            validationReport.get("errors").add("defaultConfig does not exist");
        }

        // ATTRIBUTES:
        if (configuration.getAttributes() != null) {
            for (Map.Entry<String, Configuration.Generator.Attribute> attribute : configuration.getAttributes().entrySet()) {
                // Breadcrumbs
                String breadcrumbs = ConfigurationHandler.ATTRIBUTES + "." + attribute.getKey();
                // CONFIG
                boolean gc = valGeneratorConfig(validationReport,
                        breadcrumbs,
                        configuration,
                        attribute.getValue().getConfig());
                // DATAPATHS
                boolean dps = valDataPaths(validationReport,
                        breadcrumbs,
                        attribute.getValue());
                // ATTRIBUTE
                if (gc && dps) {
                    if (attribute.getValue().getInsert() == null) {
                        validationReport.get("errors").add(breadcrumbs + ".insert: insert block is missing.");
                    } else {
                        valAttributeGenerator(validationReport,
                                breadcrumbs,
                                configuration,
                                attribute.getValue(),
                                session,
                                false);
                    }
                }
            }
        }

        // validate entities:
        if (configuration.getEntities() != null) {
            for (Map.Entry<String, Configuration.Generator.Entity> entity : configuration.getEntities().entrySet()) {
                // Breadcrumbs
                String breadcrumbs = ConfigurationHandler.ENTITIES + "." + entity.getKey();
                // CONFIG
                boolean gc = valGeneratorConfig(validationReport,
                        breadcrumbs,
                        configuration,
                        entity.getValue().getConfig());
                // DATAPATHS
                boolean dps = valDataPaths(validationReport,
                        breadcrumbs,
                        entity.getValue());
                // ENTITY
                if (gc && dps) {
                    if (entity.getValue().getInsert() == null) {
                        validationReport.get("errors").add(breadcrumbs + ".insert: insert block is missing.");
                    } else {
                        String entityType = entity.getValue().getInsert().getEntity();
                        boolean entityExists = valConceptTypeInSchema(validationReport, session, breadcrumbs, entityType, "entity");
                        if (entityExists) {
                            valEntityHasAttributes(validationReport,
                                    breadcrumbs,
                                    configuration,
                                    entity.getValue(),
                                    entity.getValue().getInsert().getOwnerships(),
                                    session);
                        }
                    }
                }
            }
        }

        // validate relations:

        if (configuration.getRelations() != null) {
            for (Map.Entry<String, Configuration.Generator.Relation> relation : configuration.getRelations().entrySet()) {
                // Breadcrumbs
                String breadcrumbs = ConfigurationHandler.RELATIONS + "." + relation.getKey();
                // CONFIG
                boolean gc = valGeneratorConfig(validationReport,
                        breadcrumbs,
                        configuration,
                        relation.getValue().getConfig());
                // DATAPATHS
                boolean dps = valDataPaths(validationReport,
                        breadcrumbs,
                        relation.getValue());
                // RELATION
                if (gc && dps) {
                    if (relation.getValue().getInsert() == null) {
                        validationReport.get("errors").add(breadcrumbs + ".insert: insert block is missing.");
                    } else {
                        String rel = relation.getValue().getInsert().getRelation();
                        boolean relationExists = valConceptTypeInSchema(validationReport, session, breadcrumbs, rel, "relation");
                        if (relationExists) {
                            valRelationHasAttributes(validationReport,
                                    breadcrumbs,
                                    configuration,
                                    relation.getValue(),
                                    relation.getValue().getInsert().getOwnerships(),
                                    session);
                            valRelationPlayers(validationReport,
                                    breadcrumbs,
                                    relation.getValue(),
                                    session);
                        }
                    }
                }
            }
        }

        // validate appendAttribute:
        if (configuration.getAppendAttribute() != null) {
            for (Map.Entry<String, Configuration.Generator.AppendAttribute> appendAttribute : configuration.getAppendAttribute().entrySet()) {
                // Breadcrumbs
                String breadcrumbs = ConfigurationHandler.APPEND_ATTRIBUTE + "." + appendAttribute.getKey();
                // CONFIG
                boolean gc = valGeneratorConfig(validationReport, breadcrumbs, configuration, appendAttribute.getValue().getConfig());
                // DATAPATHS
                boolean dps = valDataPaths(validationReport, breadcrumbs, appendAttribute.getValue());
                // APPEND_ATTRIBUTE
                if (gc && dps) {
                    // go through match
                    if (appendAttribute.getValue().getMatch() == null) {
                        validationReport.get("errors").add(breadcrumbs + ".match: match block is missing.");
                    } else {
                        String conceptType = appendAttribute.getValue().getMatch().getType();
                        boolean thingExists = valConceptTypeInSchema(validationReport, session, breadcrumbs, conceptType, "type");
                        if (thingExists) {
                            valOwnerships(validationReport,
                                    breadcrumbs + ".match",
                                    configuration,
                                    appendAttribute.getValue(),
                                    appendAttribute.getValue().getMatch().getOwnerships(),
                                    session,
                                    false);
                            if (appendAttribute.getValue().getInsert() == null) {
                                validationReport.get("errors").add(breadcrumbs + ".insert: insert block is missing.");
                            } else {
                                valOwnerships(validationReport,
                                        breadcrumbs + ".insert",
                                        configuration,
                                        appendAttribute.getValue(),
                                        appendAttribute.getValue().getMatch().getOwnerships(),
                                        session,
                                        true);
                            }
                        }
                    }
                }
            }
        }

        // validate appendOrInsertAttribute:
        if (configuration.getAppendAttributeOrInsertThing() != null) {
            for (Map.Entry<String, Configuration.Generator.AppendAttributeOrInsertThing> appendOrInsertAttribute : configuration.getAppendAttributeOrInsertThing().entrySet()) {
                // Breadcrumbs
                String breadcrumbs = ConfigurationHandler.APPEND_ATTRIBUTE_OR_INSERT_THING + "." + appendOrInsertAttribute.getKey();
                // CONFIG
                boolean gc = valGeneratorConfig(validationReport, breadcrumbs, configuration, appendOrInsertAttribute.getValue().getConfig());
                // DATAPATHS
                boolean dps = valDataPaths(validationReport, breadcrumbs, appendOrInsertAttribute.getValue());
                // APPEND_ATTRIBUTE
                if (gc && dps) {
                    // go through match
                    if (appendOrInsertAttribute.getValue().getMatch() == null) {
                        validationReport.get("errors").add(breadcrumbs + ".match: match block is missing.");
                    } else {
                        String conceptType = appendOrInsertAttribute.getValue().getMatch().getType();
                        boolean thingExists = valConceptTypeInSchema(validationReport, session, breadcrumbs, conceptType, "type");
                        if (thingExists) {
                            valOwnerships(validationReport,
                                    breadcrumbs + ".match",
                                    configuration,
                                    appendOrInsertAttribute.getValue(),
                                    appendOrInsertAttribute.getValue().getMatch().getOwnerships(),
                                    session,
                                    false);
                            if (appendOrInsertAttribute.getValue().getInsert() == null) {
                                validationReport.get("errors").add(breadcrumbs + ".insert: insert block is missing.");
                            } else {
                                valOwnerships(validationReport,
                                        breadcrumbs + ".insert",
                                        configuration,
                                        appendOrInsertAttribute.getValue(),
                                        appendOrInsertAttribute.getValue().getMatch().getOwnerships(),
                                        session,
                                        true);
                            }
                        }
                    }
                }
            }
        }
    }

    public boolean valGeneratorConfig(HashMap<String, ArrayList<String>> validationReport,
                                      String breadcrumbs,
                                      Configuration dc,
                                      Configuration.Generator.GeneratorConfig config) {
        boolean valid = true;
        breadcrumbs = breadcrumbs + ".config";
        if (Util.getSeparator(dc, config) == null) {
            validationReport.get("error").add(breadcrumbs + ".separator: missing required field: file separator must be specified here or in defaultConfig");
            valid = false;
        }
        if (Util.getRowsPerCommit(dc, config) == null) {
            validationReport.get("error").add(breadcrumbs + ".rowsPerCommit: missing required field: rowsPerCommit must be specified here or in defaultConfig");
            valid = false;
        }
        return valid;
    }

    public void validateSchemaPresent(HashMap<String, ArrayList<String>> validationReport) {
        String schema = configuration.getGlobalConfig().getSchema();
        if (schema == null) {
            validationReport.get("errors").add("defaultConfig.schema: missing required field");
        } else {
            try {
                Util.newBufferedReader(schema);
            } catch (FileNotFoundException fileNotFoundException) {
                validationReport.get("errors").add("defaultConfig.schema - schema file not found under: <" + schema + ">");
            }
        }
    }

    private boolean valDataPaths(HashMap<String, ArrayList<String>> validationReport,
                                 String breadcrumbs,
                                 Configuration.Generator generator) {
        boolean valid = true;

        String[] data = generator.getData();

        // data missing
        if (data == null) {
            validationReport.get("errors").add(breadcrumbs + ".data: missing required field");
            valid = false;
        }
        // data empty
        if (data != null && data.length < 1) {
            validationReport.get("errors").add(breadcrumbs + ".data: provided empty list - must have at least one file");
            valid = false;
        }

        // file missing or empty
        Character fileSeparator = Util.getSeparator(configuration, generator.getConfig());
        if (data != null && fileSeparator != null) {
            for (String filepath : data) {
                try {
                    Util.getFileHeader(filepath, fileSeparator);
                } catch (IOException fileNotFoundException) {
                    validationReport.get("errors").add(breadcrumbs + ".data: <" + filepath + ">: file not found");
                    valid = false;
                } catch (NullPointerException nullPointerException) {
                    validationReport.get("errors").add(breadcrumbs + ".data: <" + filepath + ">: file is empty");
                    valid = false;
                }
            }
        }
        return valid;
    }

    public void valAttributeGenerator(HashMap<String, ArrayList<String>> validationReport,
                                      String breadcrumbs,
                                      Configuration configuration,
                                      Configuration.Generator.Attribute generator,
                                      TypeDBSession session,
                                      boolean isInsert) {
        breadcrumbs = breadcrumbs + ".insert";
        if (generator.getInsert() == null) {
            validationReport.get("error").add(breadcrumbs + ": missing required insert object.");
        } else {
            valConstrainingAttribute(validationReport, breadcrumbs, configuration, generator, generator.getInsert(), session, isInsert);
        }

    }

    public void valConstrainingAttribute(HashMap<String, ArrayList<String>> validationReport,
                                         String breadcrumbs,
                                         Configuration configuration,
                                         Configuration.Generator generator,
                                         Configuration.Definition.Attribute attribute,
                                         TypeDBSession session,
                                         boolean isInsert) {
        if (attribute.getAttribute() == null) {
            validationReport.get("error").add(breadcrumbs + ".attribute: missing required field");
        } else {
            valConceptTypeInSchema(validationReport, session, breadcrumbs, attribute.getAttribute(), "attribute");
        }
        if (attribute.getColumn() == null) {
            validationReport.get("error").add(breadcrumbs + ".column: missing required field");
        } else {
            valColumnInHeader(validationReport, breadcrumbs, configuration, generator, attribute.getColumn());
        }
        if (isInsert) {
            if (attribute.getRequired() == null) {
                validationReport.get("warnings").add(breadcrumbs + ".required: field not set - defaults to false");
            }
        }
    }

    private boolean valConceptTypeInSchema(HashMap<String, ArrayList<String>> validationReport,
                                           TypeDBSession session,
                                           String breadcrumbs,
                                           String conceptType,
                                           String breadcrumbConceptType) {
        boolean exists = false;
        TypeQLMatch query = TypeQL.match(TypeQL.var("t").type(conceptType));
        try (TypeDBTransaction txn = session.transaction(TypeDBTransaction.Type.READ)) {
            Util.trace(Integer.toString((int) txn.query().match(query).count()));
            exists = true;
        } catch (TypeDBClientException typeDBClientException) {
            if (typeDBClientException.toString().contains("Invalid Type Read: The type '" + conceptType + "' does not exist.")) {
                validationReport.get("errors").add(breadcrumbs + "." + breadcrumbConceptType + ": <" + conceptType + "> does not exist in schema");
            } else {
                throw typeDBClientException;
            }
        }
        return exists;
    }

    private void valColumnInHeader(HashMap<String, ArrayList<String>> validationReport,
                                   String breadcrumbs,
                                   Configuration configuration,
                                   Configuration.Generator generator,
                                   String column) {
        for (String dataPath : generator.getData()) {
            try {
                String[] header = Util.getFileHeader(dataPath, Util.getSeparator(configuration, generator.getConfig()));
                if (Arrays.stream(header).noneMatch(headerColumn -> headerColumn.equals(column))) {
                    validationReport.get("errors").add(breadcrumbs + ".column: <" + column + "> column not found in header of file <" + dataPath + ">");
                }
            } catch (IOException | IllegalArgumentException ignored) {
            }
        }
    }


    private void valEntityHasAttributes(HashMap<String, ArrayList<String>> validationReport,
                                        String breadcrumbs,
                                        Configuration configuration,
                                        Configuration.Generator generator,
                                        Configuration.Definition.Attribute[] attributes,
                                        TypeDBSession session) {
        if (attributes == null) {
            validationReport.get("errors").add(breadcrumbs + ".ownerships: missing required ownerships list");
        } else {
            valOwnerships(validationReport, breadcrumbs, configuration, generator, attributes, session, true);
        }
    }

    private void valOwnerships(HashMap<String, ArrayList<String>> validationReport,
                               String breadcrumbs,
                               Configuration configuration,
                               Configuration.Generator generator,
                               Configuration.Definition.Attribute[] attributes,
                               TypeDBSession session,
                               boolean isInsert) {
        int entryIdx = 0;
        for (Configuration.Definition.Attribute attribute : attributes) {
            String aBreadcrumbs = breadcrumbs + ".ownerships.[" + entryIdx + "]";
            valConstrainingAttribute(validationReport, aBreadcrumbs, configuration, generator, attribute, session, isInsert);
            entryIdx += 1;
        }
    }

    private void valRelationHasAttributes(HashMap<String, ArrayList<String>> validationReport,
                                          String breadcrumbs,
                                          Configuration configuration,
                                          Configuration.Generator generator,
                                          Configuration.Definition.Attribute[] attributes,
                                          TypeDBSession session) {
        if (attributes != null) {
            valOwnerships(validationReport, breadcrumbs, configuration, generator, attributes, session, true);
        }
    }

    private void valRelationPlayers(HashMap<String, ArrayList<String>> validationReport,
                                    String breadcrumbs,
                                    Configuration.Generator.Relation relation,
                                    TypeDBSession session) {
        int playerIdx = 0;
        for (Configuration.Definition.Player player : relation.getInsert().getPlayers()) {
            String pBreadcrumbs = breadcrumbs + ".players.[" + playerIdx + "]";
            if (player.getMatch() != null) {
                if (Util.playerType(player).equals("attribute")) {
                    valAttributePlayer(validationReport, pBreadcrumbs, relation.getInsert().getRelation(), player, session);
                } else if (Util.playerType(player).equals("byAttribute")) {
                    valByAttributePlayer(validationReport, pBreadcrumbs, relation.getInsert().getRelation(), player, session);
                } else if (Util.playerType(player).equals("byPlayer")) {
                    valByPlayerPlayer(validationReport, pBreadcrumbs, relation.getInsert().getRelation(), player, session);
                } else {
                    validationReport.get("errors").add(pBreadcrumbs + ".match: missing field - must contain either an \"attribute\", \"ownerships\", or \"players\" field.");
                }
            } else {
                validationReport.get("errors").add(pBreadcrumbs + ".match: missing required field");
            }
            playerIdx += 1;
        }
    }

    private void valPlayerBasics(HashMap<String, ArrayList<String>> validationReport,
                                 String breadcrumbs,
                                 String relation,
                                 Configuration.Definition.Player player,
                                 TypeDBSession session) {
        // does role exist?
        if (player.getRole() == null) {
            validationReport.get("errors").add(breadcrumbs + ".role: missing required field");
        } else {
            valRoleType(validationReport, session, breadcrumbs, relation, player.getRole());
            valRolePlayedByConcept(validationReport, session, breadcrumbs, relation, player.getRole(), player.getMatch().getType());
        }

        //validate type exists
        valConceptTypeInSchema(validationReport, session, breadcrumbs + ".match", player.getMatch().getType(), "type");

    }

    private void valAttributePlayer(HashMap<String, ArrayList<String>> validationReport,
                                    String breadcrumbs,
                                    String relation,
                                    Configuration.Definition.Player player,
                                    TypeDBSession session) {
        valPlayerBasics(validationReport, breadcrumbs, relation, player, session);
    }

    private void valByAttributePlayer(HashMap<String, ArrayList<String>> validationReport,
                                      String breadcrumbs,
                                      String relation,
                                      Configuration.Definition.Player player,
                                      TypeDBSession session) {
        valPlayerBasics(validationReport, breadcrumbs, relation, player, session);
        // ownerships present (or wouldn't be here), but validate all attributes:
        Configuration.Definition.Attribute[] ownerships = player.getMatch().getOwnerships();
        for (Configuration.Definition.Attribute att : ownerships) {
            valConceptTypeInSchema(validationReport, session, breadcrumbs + ".ownerships", att.getAttribute(), "attribute");
        }
    }

    private void valByPlayerPlayer(HashMap<String, ArrayList<String>> validationReport,
                                   String breadcrumbs,
                                   String relation,
                                   Configuration.Definition.Player player,
                                   TypeDBSession session) {
        if (Util.playerType(player).equals("attribute")) {
            valAttributePlayer(validationReport, breadcrumbs, relation, player, session);
        } else if (Util.playerType(player).equals("byAttribute")) {
            valByAttributePlayer(validationReport, breadcrumbs, relation, player, session);
        } else if (Util.playerType(player).equals("byPlayer")) {
            for (Configuration.Definition.Player curPlayer : player.getMatch().getPlayers()) {
                valPlayerBasics(validationReport, breadcrumbs, relation, player, session);
                valByPlayerPlayer(validationReport, breadcrumbs, player.getMatch().getType(), curPlayer, session);
            }
        }
    }

    private void valRoleType(HashMap<String, ArrayList<String>> validationReport,
                             TypeDBSession session,
                             String breadcrumbs,
                             String relationType,
                             String roleType) {
        TypeQLMatch query = TypeQL.match(TypeQL.type(relationType).relates(TypeQL.var("r"))).get("r");
        try (TypeDBTransaction txn = session.transaction(TypeDBTransaction.Type.READ)) {
            Stream<ConceptMap> answers = txn.query().match(query);
            if (answers.noneMatch(a -> a.get("r").asRoleType().getLabel().name().equals(roleType))) {
                validationReport.get("errors").add(breadcrumbs + ".role: <" + roleType + "> is not a role for relation of type <" + relationType + "> in schema");
            }
        }
    }

    private void valRolePlayedByConcept(HashMap<String, ArrayList<String>> validationReport,
                                        TypeDBSession session,
                                        String breadcrumbs,
                                        String relationType,
                                        String role,
                                        String conceptType) {
        TypeQLMatch query = TypeQL.match(TypeQL.var("c").plays(relationType, role)).get("c");
        try (TypeDBTransaction txn = session.transaction(TypeDBTransaction.Type.READ)) {
            Stream<ConceptMap> answers = txn.query().match(query);
            if (answers.noneMatch(c -> c.get("c").asThingType().getLabel().name().equals(conceptType))) {
                validationReport.get("errors").add(breadcrumbs + ".role: <" + role + "> is not player by <" + conceptType + "> in relation of type <" + relationType + "> in schema");
            }
        }
    }

}
