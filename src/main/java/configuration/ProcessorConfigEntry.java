package configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import processor.AttributeValueType;
import processor.ProcessorType;

import java.util.HashMap;
import java.util.Map;

public class ProcessorConfigEntry {

    //TODO: clean up hashmap of hashmap stuff...
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");

    private String processor;
    private String processorType;
    private String schemaType;
    private String referenceProcessor;

    // top-level HashMap has single entry "conceptGenerators"
    // second-level HashMap has two entries: "attributes" and "players" (for relations only)
    // third level HashMap has one entry per attribute/player generator
    private HashMap<String, HashMap<String, ConceptProcessor>> conceptProcessors;

    public String getProcessor() {
        return processor;
    }

    public ProcessorType getProcessorType() {
        try {
            return ProcessorType.valueOf(processorType.toUpperCase().replaceAll("-", "_"));
        } catch (IllegalArgumentException illegalArgumentException) {
            return ProcessorType.INVALID;
        }
    }

    public String getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(String schemaType) {
        this.schemaType = schemaType;
    }

    public String getReferenceProcessor() {
        return referenceProcessor;
    }

    public HashMap<String, HashMap<String, ConceptProcessor>> getConceptProcessors() {
        return conceptProcessors;
    }

    public void setConceptProcessors(HashMap<String, HashMap<String, ConceptProcessor>> conceptProcessors) {
        this.conceptProcessors = conceptProcessors;
    }

    public ConceptProcessor getAttributeGenerator(String key) {
        if (getConceptProcessors().get("attributes") == null) {
            throw new RuntimeException("You have specified the attribute [" + key + "] in your dataConfig file - but there are no attributeGenerators specified in the corresponding processor.");
        }
        for (Map.Entry<String, ConceptProcessor> entry : getConceptProcessors().get("attributes").entrySet()) {
            if (entry.getKey().equals(key)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("cannot find <" + key + "> under <conceptGenerators><attributes> in processor: " + getProcessor());
    }

    public ConceptProcessor getPlayerGenerator(String key) {
        for (Map.Entry<String, ConceptProcessor> entry : getConceptProcessors().get("players").entrySet()) {
            if (entry.getKey().equals(key)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("cannot find <" + key + "> under <conceptGenerators><players> in processor: " + getProcessor());
    }

    public ConceptProcessor getRelationPlayerGenerator(String key) {
        for (Map.Entry<String, ConceptProcessor> entry : getConceptProcessors().get("relationPlayers").entrySet()) {
            if (entry.getKey().equals(key)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("cannot find <" + key + "> under <conceptGenerators><players> in processor: " + getProcessor());
    }

    public HashMap<String, ConceptProcessor> getRelationRequiredPlayers() {
        HashMap<String, ConceptProcessor> relationPlayers = new HashMap<>();
        if (processorType.equals(ProcessorType.RELATION.toString()) || processorType.equals(ProcessorType.NESTED_RELATION.toString()) || processorType.equals(ProcessorType.ATTRIBUTE_RELATION.toString())) {
            HashMap<String, ConceptProcessor> playerGenerators = getConceptProcessors().get("players");
            for (Map.Entry<String, ConceptProcessor> pg : playerGenerators.entrySet()) {
                if (pg.getValue().isRequired()) {
                    relationPlayers.put(pg.getKey(), pg.getValue());
                }
            }
            if (getConceptProcessors().get("relationPlayers") != null) {
                HashMap<String, ConceptProcessor> relationPlayerGenerators = getConceptProcessors().get("relationPlayers");
                for (Map.Entry<String, ConceptProcessor> pg : relationPlayerGenerators.entrySet()) {
                    if (pg.getValue().isRequired()) {
                        relationPlayers.put(pg.getKey(), pg.getValue());
                    }
                }
            }
        }
        return relationPlayers;
    }

    public HashMap<String, ConceptProcessor> getRelationPlayers() {
        HashMap<String, ConceptProcessor> relationPlayers = new HashMap<>();
        if (processorType.equals(ProcessorType.RELATION.toString())) {
            HashMap<String, ConceptProcessor> playerGenerators = getConceptProcessors().get("players");
            for (Map.Entry<String, ConceptProcessor> pg : playerGenerators.entrySet()) {
                relationPlayers.put(pg.getKey(), pg.getValue());
            }
        }
        return relationPlayers;
    }

    public HashMap<String, ConceptProcessor> getAttributes() {
        HashMap<String, ConceptProcessor> attributes = new HashMap<>();
        HashMap<String, ConceptProcessor> attGenerators = getConceptProcessors().get("attributes");
        for (Map.Entry<String, ConceptProcessor> ag : attGenerators.entrySet()) {
            attributes.put(ag.getKey(), ag.getValue());
        }
        return attributes;
    }

    public HashMap<String, ConceptProcessor> getRequiredAttributes() {
        HashMap<String, ConceptProcessor> requiredAttributes = new HashMap<>();
        HashMap<String, ConceptProcessor> attGenerators = getConceptProcessors().get("attributes");
        if (attGenerators != null) {
            for (Map.Entry<String, ConceptProcessor> ag : attGenerators.entrySet()) {
                if (ag.getValue().isRequired()) {
                    requiredAttributes.put(ag.getKey(), ag.getValue());
                }
            }
        }
        return requiredAttributes;
    }

    public static class ConceptProcessor {

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

        // for relationPlayers
        private HashMap<String, MatchBy> matchByPlayer;
        private HashMap<String, MatchBy> matchByAttribute;

        public boolean isRequired() {
            return required;
        }

        public String getAttributeType() {
            return attributeType;
        }

        public AttributeValueType getValueType() {
            return AttributeValueType.valueOf(valueType.toUpperCase());
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

        public AttributeValueType getIdValueType() {
            return AttributeValueType.valueOf(idValueType.toUpperCase());
        }

        public HashMap<String, MatchBy> getMatchByPlayer() {
            return matchByPlayer;
        }

        public HashMap<String, MatchBy> getMatchByAttribute() {
            return matchByAttribute;
        }

        public static class MatchBy {
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

            public AttributeValueType getValueType() {
                try {
                    return AttributeValueType.valueOf(valueType.toUpperCase());
                } catch (IllegalArgumentException illegalArgumentException) {
                    return AttributeValueType.INVALID;
                }
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

            public AttributeValueType getIdValueType() {
                try {
                    return AttributeValueType.valueOf(idValueType.toUpperCase());
                } catch (IllegalArgumentException illegalArgumentException) {
                    return AttributeValueType.INVALID;
                }
            }
        }
    }

}
