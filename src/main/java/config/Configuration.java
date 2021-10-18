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

    public Generator getGeneratorByKey(String key){
        for (Map.Entry<String, Configuration.Attribute> generator : getAttributes().entrySet()) {
            if (generator.getKey().equals(key)) {
                return generator.getValue();
            }
        }
        for (Map.Entry<String, Configuration.Entity> generator : getEntities().entrySet()) {
            if (generator.getKey().equals(key)) {
                return generator.getValue();
            }
        }
        for (Map.Entry<String, Configuration.Relation> generator : getRelations().entrySet()) {
            if (generator.getKey().equals(key)) {
                return generator.getValue();
            }
        }
        for (Map.Entry<String, Configuration.AppendAttribute> generator : getAppendAttribute().entrySet()) {
            if (generator.getKey().equals(key)) {
                return generator.getValue();
            }
        }
        for (Map.Entry<String, Configuration.AppendAttributeOrInsertThing> generator : getAppendAttributeOrInsertThing().entrySet()) {
            if (generator.getKey().equals(key)) {
                return generator.getValue();
            }
        }
        return null;
    }

    public String getGeneratorTypeByKey(String key){
        for (Map.Entry<String, Configuration.Attribute> generator : getAttributes().entrySet()) {
            if (generator.getKey().equals(key)) {
                return "attributes";
            }
        }
        for (Map.Entry<String, Configuration.Entity> generator : getEntities().entrySet()) {
            if (generator.getKey().equals(key)) {
                return "entities";
            }
        }
        for (Map.Entry<String, Configuration.Relation> generator : getRelations().entrySet()) {
            if (generator.getKey().equals(key)) {
                return "relations";
            }
        }
        for (Map.Entry<String, Configuration.AppendAttribute> generator : getAppendAttribute().entrySet()) {
            if (generator.getKey().equals(key)) {
                return "appendAttribute";
            }
        }
        for (Map.Entry<String, Configuration.AppendAttributeOrInsertThing> generator : getAppendAttributeOrInsertThing().entrySet()) {
            if (generator.getKey().equals(key)) {
                return "appendAttributeOrInsertThing";
            }
        }
        return null;
    }


    public static class GlobalConfig {

        private static final int DEFAULT_PARALLELISATION = Runtime.getRuntime().availableProcessors() * 8;

        Character separator;
        Integer rowsPerCommit;
        Integer parallelisation;
        String schemaPath;
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

        public String getSchemaPath() {
            return schemaPath;
        }

        public ArrayList<String> getOrderedAfterGenerators() {
            return orderedAfterGenerators;
        }

        public ArrayList<String> getOrderedBeforeGenerators() {
            return orderedBeforeGenerators;
        }

        public ArrayList<String> getIgnoreGenerators() { return ignoreGenerators; }
    }

    public class Generator {
        String[] dataPaths;
        GeneratorConfig config;

        public String[] getDataPaths() {
            return dataPaths;
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
        ConstrainingAttribute attribute;

        public ConstrainingAttribute getAttribute() {
            return attribute;
        }
    }

    public class Entity extends Generator {
        String conceptType;
        ConstrainingAttribute[] attributes;

        public String getConceptType() {
            return conceptType;
        }

        public ConstrainingAttribute[] getAttributes() {
            return attributes;
        }

        public ConstrainingAttribute[] getRequireNonEmptyAttributes() {
            ArrayList<ConstrainingAttribute> tmp = new ArrayList<>();
            for (ConstrainingAttribute constrainingAttribute : getAttributes()) {
                if (constrainingAttribute.getRequireNonEmpty() != null && constrainingAttribute.getRequireNonEmpty()) {
                    tmp.add(constrainingAttribute);
                }
            }
            ConstrainingAttribute[] requireNoneEmptyAttributes = new ConstrainingAttribute[tmp.size()];
            return tmp.toArray(requireNoneEmptyAttributes);
        }
    }

    public class Relation extends Generator {
        String conceptType;
        ConstrainingAttribute[] attributes;
        Player[] players;

        public String getConceptType() {
            return conceptType;
        }

        public ConstrainingAttribute[] getAttributes() {
            return attributes;
        }

        public ConstrainingAttribute[] getRequireNonEmptyAttributes() {
            ArrayList<ConstrainingAttribute> tmp = new ArrayList<>();
            if (getAttributes() != null) {
                for (ConstrainingAttribute constrainingAttribute : getAttributes()) {
                    if (constrainingAttribute.getRequireNonEmpty() != null && constrainingAttribute.getRequireNonEmpty()) {
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

        public Player[] getRequiredNonEmptyPlayers() {
            ArrayList<Player> tmp = new ArrayList<>();
            for (Player player : getPlayers()) {
                if (player.getRequireNonEmpty() != null) {
                    if (player.getRequireNonEmpty()) {
                        tmp.add(player);
                    }
                }
            }
            Player[] requireNoneEmptyAttributes = new Player[tmp.size()];
            return tmp.toArray(requireNoneEmptyAttributes);
        }
    }

    public class AppendAttribute extends Generator {
        ThingGetter thingGetter;
        ConstrainingAttribute[] attributes;

        public ConstrainingAttribute[] getAttributes() {
            return attributes;
        }

        public ConstrainingAttribute[] getRequireNonEmptyAttributes() {
            ArrayList<ConstrainingAttribute> tmp = new ArrayList<>();
            for (ConstrainingAttribute constrainingAttribute : getAttributes()) {
                if (constrainingAttribute.getRequireNonEmpty() != null && constrainingAttribute.getRequireNonEmpty()) {
                    tmp.add(constrainingAttribute);
                }
            }
            ConstrainingAttribute[] requireNoneEmptyAttributes = new ConstrainingAttribute[tmp.size()];
            return tmp.toArray(requireNoneEmptyAttributes);
        }

        public ThingGetter getThingGetter() {
            return thingGetter;
        }
    }

    public class AppendAttributeOrInsertThing extends AppendAttribute {
    }

    public class ConstrainingAttribute {
        String conceptType;
        AttributeValueType conceptValueType;
        String column;
        Boolean requireNonEmpty;
        String listSeparator;
        PreprocessorConfig preprocessorConfig;

        public String getConceptType() {
            return conceptType;
        }

        public AttributeValueType getConceptValueType() {
            return conceptValueType;
        }

        public void setConceptValueType(TypeDBTransaction txn) {
            this.conceptValueType = getValueType(txn, conceptType);
        }

        public String getColumn() {
            return column;
        }

        public Boolean getRequireNonEmpty() {
            return requireNonEmpty;
        }

        public String getListSeparator() {
            return listSeparator;
        }

        public PreprocessorConfig getPreprocessorConfig() {
            return preprocessorConfig;
        }
    }

    public class RoleGetter extends ConstrainingAttribute {
        String handler;
        ThingGetter[] thingGetters;

        public TypeHandler getHandler() {
            return TypeHandler.valueOf(handler.toUpperCase());
        }

        public ThingGetter[] getThingGetters() {
            return thingGetters;
        }

        public ThingGetter[] getOwnershipThingGetters() {
            ArrayList<ThingGetter> tmp = new ArrayList<>();
            ThingGetter[] thingGetters = getThingGetters();
            if (thingGetters != null) {
                for (ThingGetter thingGetter : getThingGetters()) {
                    if (thingGetter.getHandler() == TypeHandler.OWNERSHIP) tmp.add(thingGetter);
                }
                ThingGetter[] ownershipThingGetter = new ThingGetter[tmp.size()];
                return tmp.toArray(ownershipThingGetter);
            } else {
                return null;
            }
        }
    }

    public class ThingGetter extends RoleGetter {
        String roleType;
        //TODO Config validation: allow here only a single or a set of attributes, or a single or a set of entities/attributes, not yet a mixture

        public String getRoleType() {
            return roleType;
        }
    }

    public class Player {
        String roleType;
        Boolean requireNonEmpty;
        RoleGetter roleGetter;

        public String getRoleType() {
            return roleType;
        }

        public Boolean getRequireNonEmpty() {
            return requireNonEmpty;
        }

        public RoleGetter getRoleGetter() {
            return roleGetter;
        }
    }

    public class PreprocessorConfig {
        private String type;
        private PreprocessorParameters parameters;

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
}
