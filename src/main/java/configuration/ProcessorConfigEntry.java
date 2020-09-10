package configuration;
import java.util.HashMap;
import java.util.Map;

public class ProcessorConfigEntry {

    private String processor;
    private String processorType;
    private String schemaType;

    // top-level HashMap has single entry "conceptGenerators"
    // second-level HashMap has two entries: "attributes" and "players" (for relations only)
    // third level HashMap has one entry per attribute/player generator
    private HashMap<String, HashMap<String, ConceptGenerator>> conceptGenerators;

    public String getProcessor() {
        return processor;
    }

    public String getProcessorType() {
        return processorType;
    }

    public String getSchemaType() {
        return schemaType;
    }

    public HashMap<String, HashMap<String, ConceptGenerator>> getConceptGenerators() {
        return conceptGenerators;
    }

    // TODO: Use streaming API
    public ConceptGenerator getAttributeGenerator(String key) {
        for (Map.Entry<String, ConceptGenerator> entry : getConceptGenerators().get("attributes").entrySet()) {
            if (entry.getKey().equals(key)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("cannot find <" + key + "> under <conceptGenerators><attributes> in processor: " + getProcessor());
    }

    public ConceptGenerator getPlayerGenerator(String key) {
        for (Map.Entry<String, ConceptGenerator> entry : getConceptGenerators().get("players").entrySet()) {
            if (entry.getKey().equals(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    public static class ConceptGenerator {

        // for attributes and players:
        private boolean required;

        // for attributes
        private String attributeType;
        private String valueType;

        // for players
        private String playerType;
        private String roleType;
        private String uniquePlayerId;
        private String idValueType;

        public boolean isRequired() {
            return required;
        }

        public String getAttributeType() {
            return attributeType;
        }

        public String getValueType() {
            return valueType;
        }

        public String getPlayerType() {
            return playerType;
        }

        public String getRoleType() {
            return roleType;
        }

        public String getUniquePlayerId() {
            return uniquePlayerId;
        }

        public String getIdValueType() {
            return idValueType;
        }
    }


    public HashMap<String,ConceptGenerator> getRelationRequiredPlayers() {
        HashMap<String,ConceptGenerator> relationPlayers = new HashMap<>();
        if (processorType.equals("relation")) {
            HashMap<String, ConceptGenerator> playerGenerators = getConceptGenerators().get("players");
            for (Map.Entry<String, ConceptGenerator> pg: playerGenerators.entrySet()) {
                if (pg.getValue().isRequired()) {
                    relationPlayers.put(pg.getKey(), pg.getValue());
                }
            }
        }
        return relationPlayers;
    }

    public HashMap<String,ConceptGenerator> getRelationPlayers() {
        HashMap<String,ConceptGenerator> relationPlayers = new HashMap<>();
        if (processorType.equals("relation")) {
            HashMap<String, ConceptGenerator> playerGenerators = getConceptGenerators().get("players");
            for (Map.Entry<String, ConceptGenerator> pg : playerGenerators.entrySet()) {
                relationPlayers.put(pg.getKey(), pg.getValue());
            }
        }
        return relationPlayers;
    }

    public HashMap<String,ConceptGenerator> getEntityRequiredAttributes() {
        return getRequiredAttributes();
    }

    public HashMap<String,ConceptGenerator> getEntityAttributes() {
        return getAttributes();
    }

    public HashMap<String,ConceptGenerator> getRelationRequiredAttributes() {
        return getRequiredAttributes();
    }

    public HashMap<String,ConceptGenerator> getRelationAttributes() {
        return getAttributes();
    }

    private HashMap<String,ConceptGenerator> getAttributes() {
        HashMap<String,ConceptGenerator> attributes = new HashMap<>();
        HashMap<String, ConceptGenerator> attGenerators = getConceptGenerators().get("attributes");
        for (Map.Entry<String, ConceptGenerator> ag: attGenerators.entrySet()) {
            attributes.put(ag.getKey(), ag.getValue());
        }
        return attributes;
    }

    private HashMap<String,ConceptGenerator> getRequiredAttributes() {
        HashMap<String,ConceptGenerator> requiredAttributes = new HashMap<>();
        HashMap<String, ConceptGenerator> attGenerators = getConceptGenerators().get("attributes");
        if (attGenerators != null) {
            for (Map.Entry<String, ConceptGenerator> ag: attGenerators.entrySet()) {
                if (ag.getValue().isRequired()) {
                    requiredAttributes.put(ag.getKey(), ag.getValue());
                }
            }
        }
        return requiredAttributes;
    }


}
