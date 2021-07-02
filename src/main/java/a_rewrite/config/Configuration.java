package a_rewrite.config;

import a_rewrite.type.AttributeValueType;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typedb.client.api.answer.ConceptMap;
import com.vaticle.typeql.lang.TypeQL;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Configuration {

    DefaultConfig defaultConfig;
    Map<String, Attribute> attributes;
    Map<String, Entity> entities;
    Map<String, Relation> relations;
    Map<String, AppendAttribute> appendAttribute;

    public DefaultConfig getDefaultConfig() {
        return defaultConfig;
    }

    public Map<String, Attribute> getAttributes() {
        return attributes;
    }

    public Map<String, Entity> getEntities() { return entities; }

    public Map<String, Relation> getRelations() { return relations; }

    public Map<String, AppendAttribute> getAppendAttribute() { return appendAttribute; }

    public class DefaultConfig {
        Character separator;
        Integer rowsPerCommit;
        String schemaPath;

        public Character getSeparator() {
            return separator;
        }

        public Integer getRowsPerCommit() {
            return rowsPerCommit;
        }

        public String getSchemaPath() { return schemaPath; }
    }

    public class Generator {
        String[] dataPaths;
        Character separator;
        Integer rowsPerCommit;

        public String[] getDataPaths() {
            return dataPaths;
        }

        public Character getSeparator() { return separator; }

        public Integer getRowsPerCommit() { return rowsPerCommit; }
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
                if (constrainingAttribute.getRequireNonEmpty()) {
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
                    if (constrainingAttribute.getRequireNonEmpty()) {
                        tmp.add(constrainingAttribute);
                    }
                }
                ConstrainingAttribute[] requireNoneEmptyAttributes = new ConstrainingAttribute[tmp.size()];
                return tmp.toArray(requireNoneEmptyAttributes);
            } else {
                return new ConstrainingAttribute[0];
            }

        }

        public Player[] getPlayers() { return players; }

        public Player[] getRequiredNonEmptyPlayers() {
            ArrayList<Player> tmp = new ArrayList<>();
            for (Player player : getPlayers()) {
                if (player.getRequireNonEmpty()) {
                    tmp.add(player);
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

    public class ConstrainingAttribute {
        String conceptType;
        AttributeValueType conceptValueType;
        String column;
        Boolean requireNonEmpty;
        String listSeparator;
        PreprocessorConfig preprocessorConfig;

        public void setConceptValueType(TypeDBTransaction txn) {
            this.conceptValueType = getValueType(txn, conceptType);
        }

        public String getConceptType() {
            return conceptType;
        }

        public AttributeValueType getConceptValueType() {
            return conceptValueType;
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

    public class RoleGetter extends ConstrainingAttribute{
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

        public PreprocessorParameters getParameters() { return parameters; }

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

    public static AttributeValueType getValueType(TypeDBTransaction txn, String conceptType) {
        AttributeValueType valueType = null;
        Set<ConceptMap> answers = txn.query().match(TypeQL.match(TypeQL.var("t").type(conceptType)).get("t")).collect(Collectors.toSet());
        assert answers.size() == 1;
        for (ConceptMap answer : answers) {
            valueType = AttributeValueType.valueOf(answer.get("t").asAttributeType().getValueType().name());
        }
        return valueType;
    }
}
