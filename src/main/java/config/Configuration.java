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

package config;

import com.vaticle.typedb.client.api.TypeDBSession;
import com.vaticle.typedb.client.api.TypeDBTransaction;
import com.vaticle.typedb.client.api.answer.ConceptMap;
import com.vaticle.typeql.lang.TypeQL;
import type.AttributeValueType;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Configuration {

    GlobalConfig globalConfig;
    Map<String, Generator.Attribute> attributes;
    Map<String, Generator.EntityInsert> entities;
    Map<String, Generator.Relation> relations;
    Map<String, Generator.AppendAttribute> appendAttribute;
    Map<String, Generator.AppendAttributeOrInsertThing> appendAttributeOrInsertThing;

    public static AttributeValueType getValueType(TypeDBSession session, String conceptType) {
        AttributeValueType valueType = null;
        try (TypeDBTransaction txn = session.transaction(TypeDBTransaction.Type.READ)) {
            Set<ConceptMap> answers = txn.query().match(TypeQL.match(TypeQL.var("t").type(conceptType)).get("t")).collect(Collectors.toSet());
            assert answers.size() == 1;
            for (ConceptMap answer : answers) {
                valueType = AttributeValueType.valueOf(answer.get("t").asAttributeType().getValueType().name());
            }
            return valueType;
        }
    }

    public GlobalConfig getGlobalConfig() {
        return globalConfig;
    }

    public Map<String, Generator.Attribute> getAttributes() {
        return attributes;
    }

    public Map<String, Generator.EntityInsert> getEntities() {
        return entities;
    }

    public Map<String, Generator.Relation> getRelations() {
        return relations;
    }

    public Map<String, Generator.AppendAttribute> getAppendAttribute() {
        return appendAttribute;
    }

    public Map<String, Generator.AppendAttributeOrInsertThing> getAppendAttributeOrInsertThing() {
        return appendAttributeOrInsertThing;
    }

    public Generator getGeneratorByKey(String key) {
        Map<String, Generator.Attribute> attributeGenerators = getAttributes();
        if (attributeGenerators != null && attributeGenerators.containsKey(key)) {
            return attributeGenerators.get(key);
        }
        Map<String, Generator.EntityInsert> entityGenerators = getEntities();
        if (entityGenerators != null && entityGenerators.containsKey(key)) {
            return entityGenerators.get(key);
        }
        Map<String, Generator.Relation> relationGenerators = getRelations();
        if (relationGenerators != null && relationGenerators.containsKey(key)) {
            return relationGenerators.get(key);
        }
        Map<String, Generator.AppendAttribute> aaGenerators = getAppendAttribute();
        if (aaGenerators != null && aaGenerators.containsKey(key)) {
            return aaGenerators.get(key);
        }
        Map<String, Generator.AppendAttributeOrInsertThing> aiGenerators = getAppendAttributeOrInsertThing();
        if (aiGenerators != null && aiGenerators.containsKey(key)) {
            return aiGenerators.get(key);
        }
        return null;
    }

    public String getGeneratorTypeByKey(String key) {
        Map<String, Generator.Attribute> attributeGenerators = getAttributes();
        if (attributeGenerators != null && attributeGenerators.containsKey(key)) {
            return "attributes";
        }

        Map<String, Generator.EntityInsert> entityGenerators = getEntities();
        if (entityGenerators != null && entityGenerators.containsKey(key)) {
            return "entities";
        }

        Map<String, Generator.Relation> relationGenerators = getRelations();
        if (relationGenerators != null && relationGenerators.containsKey(key)) {
            return "relations";
        }

        Map<String, Generator.AppendAttribute> aaGenerators = getAppendAttribute();
        if (aaGenerators != null && aaGenerators.containsKey(key)) {
            return "appendAttribute";
        }

        Map<String, Generator.AppendAttributeOrInsertThing> aiGenerators = getAppendAttributeOrInsertThing();
        if (aiGenerators != null && aiGenerators.containsKey(key)) {
            return "appendAttributeOrInsertThing";
        }
        return null;
    }

    public static class GlobalConfig {

        private static final int DEFAULT_PARALLELISATION = Runtime.getRuntime().availableProcessors() * 8;

        Character separator;
        Integer rowsPerCommit;
        Integer parallelisation;
        String schema;
        ArrayList<String> orderedBeforeGenerators;
        ArrayList<String> orderedAfterGenerators;
        ArrayList<String> ignoreGenerators;

        public Character getSeparator() {
            return separator;
        }

        public Integer getRowsPerCommit() {
            return rowsPerCommit;
        }

        public Integer getParallelisation() {
            if (parallelisation == null) return DEFAULT_PARALLELISATION;
            else return parallelisation;
        }

        public String getSchema() {
            return schema;
        }

        public ArrayList<String> getOrderedAfterGenerators() {
            return orderedAfterGenerators;
        }

        public ArrayList<String> getOrderedBeforeGenerators() {
            return orderedBeforeGenerators;
        }

        public ArrayList<String> getIgnoreGenerators() {
            return ignoreGenerators;
        }
    }

    public static abstract class Generator {
        String[] data;
        GeneratorConfig config;

        public String[] getData() {
            return data;
        }

        public GeneratorConfig getConfig() {
            return config;
        }

        public static class GeneratorConfig {
            Character separator;
            Integer rowsPerCommit;

            public Character getSeparator() {
                return separator;
            }

            public Integer getRowsPerCommit() {
                return rowsPerCommit;
            }
        }

        public static class Attribute extends Generator {
            Definition.Attribute insert;

            public Definition.Attribute getInsert() {
                return insert;
            }
        }

        public static class EntityInsert extends Generator {
            Insert insert;

            public Insert getInsert() {
                return insert;
            }

            public static class Insert {
                String entity;
                Definition.Attribute[] ownerships;

                public String getEntity() {
                    return entity;
                }

                public Definition.Attribute[] getOwnerships() {
                    return ownerships;
                }

                public Definition.Attribute[] getRequiredOwnerships() {
                    ArrayList<Definition.Attribute> tmp = new ArrayList<>();
                    for (Definition.Attribute attribute : getOwnerships()) {
                        if (attribute.getRequired() != null && attribute.getRequired()) {
                            tmp.add(attribute);
                        }
                    }
                    Definition.Attribute[] requireNoneEmptyAttributes = new Definition.Attribute[tmp.size()];
                    return tmp.toArray(requireNoneEmptyAttributes);
                }
            }
        }

        public static class Relation extends Generator {
            Insert insert;

            public Insert getInsert() {
                return insert;
            }

            public static class Insert {
                String relation;
                Definition.Attribute[] ownerships;
                Definition.Player[] players;

                public String getRelation() {
                    return relation;
                }

                public Definition.Attribute[] getOwnerships() {
                    return ownerships;
                }

                public Definition.Attribute[] getRequiredOwnerships() {
                    ArrayList<Definition.Attribute> tmp = new ArrayList<>();
                    if (ownerships != null) {
                        for (Definition.Attribute attribute : ownerships) {
                            if (attribute.getRequired() != null && attribute.getRequired()) {
                                tmp.add(attribute);
                            }
                        }
                        Definition.Attribute[] requireNoneEmptyAttributes = new Definition.Attribute[tmp.size()];
                        return tmp.toArray(requireNoneEmptyAttributes);
                    } else {
                        return new Definition.Attribute[0];
                    }
                }

                public Definition.Player[] getPlayers() {
                    return players;
                }

                public Definition.Player[] getRequiredPlayers() {
                    ArrayList<Definition.Player> tmp = new ArrayList<>();
                    for (Definition.Player player : getPlayers()) {
                        if (player.getRequired() != null) {
                            if (player.getRequired()) {
                                tmp.add(player);
                            }
                        }
                    }
                    Definition.Player[] requireNoneEmptyAttributes = new Definition.Player[tmp.size()];
                    return tmp.toArray(requireNoneEmptyAttributes);
                }
            }

        }

        public static class AppendAttribute extends Generator {
            Match match;
            Insert insert;

            public Match getMatch() {
                return match;
            }

            public Insert getInsert() {
                return insert;
            }

            public static class Match {
                String type;
                Definition.Attribute[] ownerships;

                public String getType() {
                    return type;
                }

                public Definition.Attribute[] getOwnerships() {
                    return ownerships;
                }
            }

            public static class Insert {
                Definition.Attribute[] ownerships;

                public Definition.Attribute[] getOwnerships() {
                    return ownerships;
                }

                public Definition.Attribute[] getRequiredOwnerships() {
                    ArrayList<Definition.Attribute> tmp = new ArrayList<>();
                    for (Definition.Attribute attribute : getOwnerships()) {
                        if (attribute.getRequired() != null && attribute.getRequired()) {
                            tmp.add(attribute);
                        }
                    }
                    Definition.Attribute[] requireNoneEmptyAttributes = new Definition.Attribute[tmp.size()];
                    return tmp.toArray(requireNoneEmptyAttributes);
                }
            }
        }

        public static class AppendAttributeOrInsertThing extends AppendAttribute {
        }

    }

    public static class PreprocessorConfig {
        String type;
        PreprocessorParameters parameters;

        public String getType() {
            return type;
        }

        public PreprocessorParameters getParameters() {
            return parameters;
        }

        public static class PreprocessorParameters {
            String regexMatch;
            String regexReplace;

            public String getRegexMatch() {
                return regexMatch;
            }

            public String getRegexReplace() {
                return regexReplace;
            }
        }
    }

    public static class Definition {

        public static class Attribute {
            String attribute;
            AttributeValueType conceptValueType;
            String column;
            Boolean required;
            String listSeparator;
            PreprocessorConfig preprocessorConfig;

            public String getAttribute() {
                return attribute;
            }

            public AttributeValueType getConceptValueType() {
                return conceptValueType;
            }

            public void setConceptValueType(TypeDBSession session) {
                this.conceptValueType = getValueType(session, attribute);
            }

            public String getColumn() {
                return column;
            }

            public Boolean getRequired() {
                return required;
            }

            public String getListSeparator() {
                return listSeparator;
            }

            public PreprocessorConfig getPreprocessorConfig() {
                return preprocessorConfig;
            }

            public void setAttribute(String attribute) {
                this.attribute = attribute;
            }
        }

        public static class Player {
            String role;
            Boolean required;
            Thing match;

            public String getRole() {
                return role;
            }

            public Boolean getRequired() {
                return required;
            }

            public Definition.Thing getMatch() {
                return match;
            }
        }

        public static class Thing {
            String type;
            Attribute attribute;
            Attribute[] ownerships;
            Player[] players;

            public String getType() {
                return type;
            }

            public Attribute[] getOwnerships() {
                return ownerships;
            }

            public Player[] getPlayers() {
                return players;
            }

            public Attribute getAttribute() {
                return attribute;
            }

            public Player[] getRequiredPlayers() {
                ArrayList<Player> tmp = new ArrayList<>();
                for (Player player : getPlayers()) {
                    if (player.getRequired() != null) {
                        if (player.getRequired()) {
                            tmp.add(player);
                        }
                    }
                }
                Player[] requiredPlayers = new Player[tmp.size()];
                return tmp.toArray(requiredPlayers);
            }
        }
    }
}
