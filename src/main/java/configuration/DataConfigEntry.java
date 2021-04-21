package configuration;

import java.util.ArrayList;

public class DataConfigEntry {
    private String[] dataPath;
    private Character separator;
    private String processor;
    private DataConfigGeneratorMapping[] attributes;
    private DataConfigGeneratorMapping[] players;
    private DataConfigGeneratorMapping[] relationPlayers;
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

    public DataConfigGeneratorMapping[] getAttributes() {
        return attributes;
    }

    public DataConfigGeneratorMapping[] getPlayers() {
        return players;
    }

    public DataConfigGeneratorMapping[] getRelationPlayers() {
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

    public ArrayList<DataConfigEntry.DataConfigGeneratorMapping> getMatchAttributes() {
        ArrayList<DataConfigEntry.DataConfigGeneratorMapping> matchAttributes = new ArrayList<>();
        for (DataConfigEntry.DataConfigGeneratorMapping attributeMapping : getAttributes()) {
            if (attributeMapping.isMatch()) {
                matchAttributes.add(attributeMapping);
            }
        }
        return matchAttributes;
    }

    public ArrayList<String> getMatchAttributeGenerators() {
        ArrayList<String> matchAttributeGenerators = new ArrayList<>();
        for (DataConfigEntry.DataConfigGeneratorMapping attributeMapping : getAttributes()) {
            if (attributeMapping.isMatch()) {
                matchAttributeGenerators.add(attributeMapping.getGenerator());
            }
        }
        return matchAttributeGenerators;
    }

    public static class DataConfigGeneratorMapping {
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

        public String getGenerator() {
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
