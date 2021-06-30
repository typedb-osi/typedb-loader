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

    public DefaultConfig getDefaultConfig() {
        return defaultConfig;
    }

    public Map<String, Attribute> getAttributes() {
        return attributes;
    }

    public Map<String, Entity> getEntities() { return entities; }

    public Map<String, Relation> getRelations() { return relations; }

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

    public class Attribute {
        String[] dataPaths;
        Character separator;
        String column;
        String listSeparator;
        PreprocessorConfig preprocessorConfig;
        String conceptType;
        AttributeValueType conceptValueType;
        Integer rowsPerCommit;

        public String[] getDataPaths() {
            return dataPaths;
        }

        public Character getSeparator() { return separator; }

        public String getColumn() {
            return column;
        }

        public String getListSeparator() { return listSeparator; }

        public PreprocessorConfig getPreprocessorConfig() { return preprocessorConfig; }

        public String getConceptType() {
            return conceptType;
        }

        public AttributeValueType getConceptValueType() {
            return conceptValueType;
        }

        public Integer getRowsPerCommit() { return rowsPerCommit; }

        public void setConceptValueType(TypeDBTransaction txn) {
            this.conceptValueType = getValueType(txn, conceptType);
        }
    }

    public class Entity {
        String[] dataPaths;
        Character separator;
        String conceptType;
        Integer rowsPerCommit;
        HasAttribute[] attributes;

        public String[] getDataPaths() {
            return dataPaths;
        }

        public Character getSeparator() {
            return separator;
        }

        public String getConceptType() {
            return conceptType;
        }

        public Integer getRowsPerCommit() {
            return rowsPerCommit;
        }

        public HasAttribute[] getAttributes() {
            return attributes;
        }

        public HasAttribute[] getRequireNonEmptyAttributes() {
            ArrayList<HasAttribute> tmp = new ArrayList<>();
            for (HasAttribute hasAttribute : getAttributes()) {
                if (hasAttribute.getRequireNonEmpty()) {
                    tmp.add(hasAttribute);
                }
            }
            HasAttribute[] requireNoneEmptyAttributes = new HasAttribute[tmp.size()];
            return tmp.toArray(requireNoneEmptyAttributes);
        }
    }

    public class Relation {
        String[] dataPaths;
        Character separator;
        String conceptType;
        Integer rowsPerCommit;
        HasAttribute[] attributes;
        Player[] players;

        public String[] getDataPaths() {
            return dataPaths;
        }

        public Character getSeparator() {
            return separator;
        }

        public String getConceptType() {
            return conceptType;
        }

        public Integer getRowsPerCommit() {
            return rowsPerCommit;
        }

        public HasAttribute[] getAttributes() {
            return attributes;
        }

        public HasAttribute[] getRequireNonEmptyAttributes() {
            ArrayList<HasAttribute> tmp = new ArrayList<>();
            for (HasAttribute hasAttribute : getAttributes()) {
                if (hasAttribute.getRequireNonEmpty()) {
                    tmp.add(hasAttribute);
                }
            }
            HasAttribute[] requireNoneEmptyAttributes = new HasAttribute[tmp.size()];
            return tmp.toArray(requireNoneEmptyAttributes);
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

    public class HasAttribute {
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

    public class Player {
        String roleType;
        Boolean requireNonEmpty;
        Getter[] getter;

        public String getRoleType() {
            return roleType;
        }

        public Boolean getRequireNonEmpty() {
            return requireNonEmpty;
        }

        public Getter[] getGetter() {
            return getter;
        }

        public Getter getAttributeGetter() {
            ArrayList<Getter> tmp = new ArrayList<>();
            for (Getter getter : getGetter()) {
                if (getter.getHandler() == TypeHandler.ATTRIBUTE) tmp.add(getter);
            }
            if (tmp.size() == 1) {
                return tmp.get(0); //TODO - log error or catch in validation!
            } else {
                return null;
            }
        }

        public Getter getEntityGetter() {
            ArrayList<Getter> tmp = new ArrayList<>();
            for (Getter getter : getGetter()) {
                if (getter.getHandler() != TypeHandler.ENTITY) tmp.add(getter);
            }
            if (tmp.size() > 1) {
                return null; //TODO - log error or catch in validation!
            } else {
                return tmp.get(0);
            }
        }

        public Getter getRelationGetter() {
            ArrayList<Getter> tmp = new ArrayList<>();
            for (Getter getter : getGetter()) {
                if (getter.getHandler() != TypeHandler.RELATION) tmp.add(getter);
            }
            if (tmp.size() > 1) {
                return null; //TODO - log error or catch in validation!
            } else {
                return tmp.get(0);
            }
        }

        public Getter[] getOwnershipGetters() {
            ArrayList<Getter> tmp = new ArrayList<>();
            for (Getter getter : getGetter()) {
                if (getter.getHandler() == TypeHandler.OWNERSHIP) tmp.add(getter);
            }
            Getter[] ownershipGetter = new Getter[tmp.size()];
            return tmp.toArray(ownershipGetter);
        }

        public Getter getRolePlayerGetter() {
            ArrayList<Getter> tmp = new ArrayList<>();
            for (Getter getter : getGetter()) {
                if (getter.getHandler() != TypeHandler.OWNERSHIP) tmp.add(getter);
            }
            if (tmp.size() > 1) {
                return null; //TODO - log error or catch in validation!
            } else {
                return tmp.get(0);
            }
        }
    }

    public class Getter {
        String conceptType;
        String handler;
        String column;
        String listSeparator;
        AttributeValueType conceptValueType;
        PreprocessorConfig preprocessorConfig;

        public String getConceptType() {
            return conceptType;
        }

        public TypeHandler getHandler() {
            return TypeHandler.valueOf(handler.toUpperCase());
        }

        public String getColumn() {
            return column;
        }

        public String getListSeparator() {
            return listSeparator;
        }

        public void setConceptValueType(TypeDBTransaction txn) {
            this.conceptValueType = getValueType(txn, conceptType);
        }

        public AttributeValueType getConceptValueType() {
            return conceptValueType;
        }

        public PreprocessorConfig getPreprocessorConfig() {
            return preprocessorConfig;
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
