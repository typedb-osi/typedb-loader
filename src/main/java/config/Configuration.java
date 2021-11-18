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

import type.AttributeValueType;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typedb.client.api.answer.ConceptMap;
import com.vaticle.typeql.lang.TypeQL;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Configuration {

    GlobalConfig globalConfig;
    Map<String, Attribute> attributes;
    Map<String, Entity> entities;
    Map<String, Relation> relations;
    Map<String, AppendAttribute> appendAttribute;
    Map<String, AppendAttributeOrInsertThing> appendAttributeOrInsertThing;

    public static AttributeValueType getValueType(TypeDBTransaction txn, String conceptType) {
        AttributeValueType valueType = null;
        Set<ConceptMap> answers = txn.query().match(TypeQL.match(TypeQL.var("t").type(conceptType)).get("t")).collect(Collectors.toSet());
        assert answers.size() == 1;
        for (ConceptMap answer : answers) {
            valueType = AttributeValueType.valueOf(answer.get("t").asAttributeType().getValueType().name());
        }
        return valueType;
    }

    public GlobalConfig getGlobalConfig() {
        return globalConfig;
    }

    public Map<String, Attribute> getAttributes() {
        return attributes;
    }

    public Map<String, Entity> getEntities() {
        return entities;
    }

    public Map<String, Relation> getRelations() {
        return relations;
    }

    public Map<String, AppendAttribute> getAppendAttribute() {
        return appendAttribute;
    }

    public Map<String, AppendAttributeOrInsertThing> getAppendAttributeOrInsertThing() {
        return appendAttributeOrInsertThing;
    }

    public Generator getGeneratorByKey(String key) {
        Map<String, Configuration.Attribute> attributeGenerators = getAttributes();
        if (attributeGenerators != null) {
            for (Map.Entry<String, Configuration.Attribute> generator : getAttributes().entrySet()) {
                if (generator.getKey().equals(key)) {
                    return generator.getValue();
                }
            }
        }
        Map<String, Configuration.Entity> entityGenerators = getEntities();
        if (entityGenerators != null) {
            for (Map.Entry<String, Configuration.Entity> generator : getEntities().entrySet()) {
                if (generator.getKey().equals(key)) {
                    return generator.getValue();
                }
            }
        }
        Map<String, Configuration.Relation> relationGenerators = getRelations();
        if (relationGenerators != null) {
            for (Map.Entry<String, Configuration.Relation> generator : getRelations().entrySet()) {
                if (generator.getKey().equals(key)) {
                    return generator.getValue();
                }
            }
        }
        Map<String, Configuration.AppendAttribute> aaGenerators = getAppendAttribute();
        if (aaGenerators != null) {
            for (Map.Entry<String, Configuration.AppendAttribute> generator : getAppendAttribute().entrySet()) {
                if (generator.getKey().equals(key)) {
                    return generator.getValue();
                }
            }
        }
        Map<String, Configuration.AppendAttributeOrInsertThing> aiGenerators = getAppendAttributeOrInsertThing();
        if (aiGenerators != null) {
            for (Map.Entry<String, Configuration.AppendAttributeOrInsertThing> generator : getAppendAttributeOrInsertThing().entrySet()) {
                if (generator.getKey().equals(key)) {
                    return generator.getValue();
                }
            }
        }
        return null;
    }

    public String getGeneratorTypeByKey(String key) {
        Map<String, Configuration.Attribute> attributeGenerators = getAttributes();
        if (attributeGenerators != null) {
            for (Map.Entry<String, Configuration.Attribute> generator : getAttributes().entrySet()) {
                if (generator.getKey().equals(key)) {
                    return "attributes";
                }
            }
        }

        Map<String, Configuration.Entity> entityGenerators = getEntities();
        if (entityGenerators != null) {
            for (Map.Entry<String, Configuration.Entity> generator : getEntities().entrySet()) {
                if (generator.getKey().equals(key)) {
                    return "entities";
                }
            }
        }

        Map<String, Configuration.Relation> relationGenerators = getRelations();
        if (relationGenerators != null) {
            for (Map.Entry<String, Configuration.Relation> generator : getRelations().entrySet()) {
                if (generator.getKey().equals(key)) {
                    return "relations";
                }
            }
        }

        Map<String, Configuration.AppendAttribute> aaGenerators = getAppendAttribute();
        if (aaGenerators != null) {
            for (Map.Entry<String, Configuration.AppendAttribute> generator : getAppendAttribute().entrySet()) {
                if (generator.getKey().equals(key)) {
                    return "appendAttribute";
                }
            }
        }

        Map<String, Configuration.AppendAttributeOrInsertThing> aiGenerators = getAppendAttributeOrInsertThing();
        if (aiGenerators != null) {
            for (Map.Entry<String, Configuration.AppendAttributeOrInsertThing> generator : getAppendAttributeOrInsertThing().entrySet()) {
                if (generator.getKey().equals(key)) {
                    return "appendAttributeOrInsertThing";
                }
            }
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

    public class Generator {
        String[] data;
        GeneratorConfig config;

        public String[] getData() {
            return data;
        }

        public GeneratorConfig getConfig() {
            return config;
        }
    }

    public class GeneratorConfig {
        Character separator;
        Integer rowsPerCommit;

        public Character getSeparator() {
            return separator;
        }

        public Integer getRowsPerCommit() {
            return rowsPerCommit;
        }
    }


    public class Attribute extends Generator {
        ConstrainingAttribute insert;

        public ConstrainingAttribute getInsert() {
            return insert;
        }
    }

    public class ConstrainingAttribute {
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

        public void setConceptValueType(TypeDBTransaction txn) {
            this.conceptValueType = getValueType(txn, attribute);
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

    public class PreprocessorConfig {
        String type;
        PreprocessorParameters parameters;

        public String getType() {
            return type;
        }

        public PreprocessorParameters getParameters() {
            return parameters;
        }

        public class PreprocessorParameters {
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

    public class Entity extends Generator {
        EntityInsert insert;

        public EntityInsert getInsert() {
            return insert;
        }
    }

    public class EntityInsert {
        String entity;
        ConstrainingAttribute[] ownerships;

        public String getEntity() {
            return entity;
        }

        public ConstrainingAttribute[] getOwnerships() {
            return ownerships;
        }

        public ConstrainingAttribute[] getRequiredOwnerships() {
            ArrayList<ConstrainingAttribute> tmp = new ArrayList<>();
            for (ConstrainingAttribute constrainingAttribute : getOwnerships()) {
                if (constrainingAttribute.getRequired() != null && constrainingAttribute.getRequired()) {
                    tmp.add(constrainingAttribute);
                }
            }
            ConstrainingAttribute[] requireNoneEmptyAttributes = new ConstrainingAttribute[tmp.size()];
            return tmp.toArray(requireNoneEmptyAttributes);
        }
    }

    public class Relation extends Generator {
        RelationInsert insert;

        public RelationInsert getInsert() {
            return insert;
        }
    }

    public class RelationInsert {
        String relation;
        ConstrainingAttribute[] ownerships;
        Player[] players;

        public String getRelation() {
            return relation;
        }

        public ConstrainingAttribute[] getOwnerships() {
            return ownerships;
        }

        public ConstrainingAttribute[] getRequiredOwnerships() {
            ArrayList<ConstrainingAttribute> tmp = new ArrayList<>();
            if (getOwnerships() != null) {
                for (ConstrainingAttribute constrainingAttribute : getOwnerships()) {
                    if (constrainingAttribute.getRequired() != null && constrainingAttribute.getRequired()) {
                        tmp.add(constrainingAttribute);
                    }
                }
                ConstrainingAttribute[] requireNoneEmptyAttributes = new ConstrainingAttribute[tmp.size()];
                return tmp.toArray(requireNoneEmptyAttributes);
            } else {
                return new ConstrainingAttribute[0];
            }
        }

        public Player[] getPlayers() {
            return players;
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
            Player[] requireNoneEmptyAttributes = new Player[tmp.size()];
            return tmp.toArray(requireNoneEmptyAttributes);
        }
    }

    public class Player {
        String role;
        Boolean required;
        RoleGetter match;

        public String getRole() {
            return role;
        }

        public Boolean getRequired() {
            return required;
        }

        public RoleGetter getMatch() {
            return match;
        }
    }

    public class RoleGetter {
        String type;
        ConstrainingAttribute attribute;
        ConstrainingAttribute[] ownerships;
        Player[] players;

        public String getType() {
            return type;
        }

        public ConstrainingAttribute[] getOwnerships() {
            return ownerships;
        }

        public Player[] getPlayers() {
            return players;
        }

        public ConstrainingAttribute getAttribute() { return attribute; }

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

    public class AppendAttribute extends Generator {
        AppendAttributeMatch match;
        AppendAttributeInsert insert;

        public AppendAttributeMatch getMatch() {
            return match;
        }

        public AppendAttributeInsert getInsert() {
            return insert;
        }
    }

    public class AppendAttributeMatch {
        String type;
        ConstrainingAttribute[] ownerships;

        public String getType() {
            return type;
        }

        public ConstrainingAttribute[] getOwnerships() {
            return ownerships;
        }
    }

    public class AppendAttributeInsert {
        ConstrainingAttribute[] ownerships;

        public ConstrainingAttribute[] getOwnerships() {
            return ownerships;
        }

        public ConstrainingAttribute[] getRequiredOwnerships() {
            ArrayList<ConstrainingAttribute> tmp = new ArrayList<>();
            for (ConstrainingAttribute constrainingAttribute : getOwnerships()) {
                if (constrainingAttribute.getRequired() != null && constrainingAttribute.getRequired()) {
                    tmp.add(constrainingAttribute);
                }
            }
            ConstrainingAttribute[] requireNoneEmptyAttributes = new ConstrainingAttribute[tmp.size()];
            return tmp.toArray(requireNoneEmptyAttributes);
        }
    }

    public class AppendAttributeOrInsertThing extends AppendAttribute { }

}
