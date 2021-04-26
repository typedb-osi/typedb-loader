package configuration;

import java.util.ArrayList;

public class ConfigEntryData {
    private String[] dataPath;
    private Character separator;
    private String processor;
    private ConceptProcessorMapping[] attributes;
    private ConceptProcessorMapping[] players;
    private ConceptProcessorMapping[] relationPlayers;
    private int batchSize;
    private int threads;
    private Integer orderBefore;
    private Integer orderAfter;

    public String[] getDataPath() {
        return dataPath;
    }

    public Character getSeparator() {
        return separator;
    }

    public String getProcessor() {
        return processor;
    }

    public ConceptProcessorMapping[] getAttributeProcessorMappings() {
        return attributes;
    }

    public ConceptProcessorMapping[] getPlayerProcessorMappings() {
        return players;
    }

    public ConceptProcessorMapping[] getRelationPlayerProcessorMappings() {
        return relationPlayers;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getThreads() {
        return threads;
    }

    public Integer getOrderBefore() {
        return orderBefore;
    }

    public Integer getOrderAfter() {
        return orderAfter;
    }

    public ArrayList<ConceptProcessorMapping> getMatchAttributes() {
        ArrayList<ConceptProcessorMapping> matchAttributes = new ArrayList<>();
        for (ConceptProcessorMapping attributeMapping : getAttributeProcessorMappings()) {
            if (attributeMapping.isMatch()) {
                matchAttributes.add(attributeMapping);
            }
        }
        return matchAttributes;
    }

    public static class ConceptProcessorMapping {
        private String columnName;
        private String[] columnNames;
        private String generator;
        private String listSeparator;
        private String matchByAttribute;
        private String[] matchByPlayers;
        private boolean match;
        private PreprocessorConfig preprocessor;

        public String getColumnName() {
            return columnName;
        }

        public String[] getColumnNames() {
            return columnNames;
        }

        public String getConceptProcessor() {
            return generator;
        }

        public String getListSeparator() {
            return listSeparator;
        }

        public String[] getMatchByPlayers() {
            return matchByPlayers;
        }

        public String getMatchByAttribute() {
            return matchByAttribute;
        }

        public boolean isMatch() {
            return match;
        }

        public PreprocessorConfig getPreprocessor() {
            return preprocessor;
        }

        public static class PreprocessorConfig {
            private String type;
            private PreprocessorParams params;

            public String getType() {
                return type;
            }

            public PreprocessorParams getParams() {
                return params;
            }

            public static class PreprocessorParams {
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
}
