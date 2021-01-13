package configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DataConfigEntry {
    private String dataPath;
    private String separator;
    private String processor;
    private DataConfigGeneratorMapping[] attributes;
    private DataConfigGeneratorMapping[] players;
    private DataConfigGeneratorMapping[] relationPlayers;
    private int batchSize;
    private int threads;

    public String getDataPath() {
        return dataPath;
    }

    public String getSeparator() {
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

    public ArrayList<DataConfigEntry.DataConfigGeneratorMapping> getMatchAttributes() {
        ArrayList<DataConfigEntry.DataConfigGeneratorMapping> matchAttributes = new ArrayList<>();
        for (DataConfigEntry.DataConfigGeneratorMapping attributeMapping: getAttributes()) {
            if (attributeMapping.isMatch()) {
                matchAttributes.add(attributeMapping);
            }
        }
        return matchAttributes;
    }

    public static class DataConfigGeneratorMapping {
        private String columnName;
        private String[] columnNames;
        private String generator;
        private String listSeparator;
        private String matchByAttribute;
        private String[] matchByPlayers;
        private boolean match;

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

        public String[] getMatchByPlayers() { return matchByPlayers; }

        public String getMatchByAttribute() { return matchByAttribute; }

        public boolean isMatch() { return match; }

    }
}
