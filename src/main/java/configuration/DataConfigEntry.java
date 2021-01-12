package configuration;

public class DataConfigEntry {
    private String dataPath;
    private String separator;
    private String processor;
    private dataConfigGeneratorMapping[] attributes;
    private dataConfigGeneratorMapping[] players;
    private dataConfigGeneratorMapping[] relationPlayers;
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

    public dataConfigGeneratorMapping[] getAttributes() {
        return attributes;
    }

    public dataConfigGeneratorMapping[] getPlayers() {
        return players;
    }

    public dataConfigGeneratorMapping[] getRelationPlayers() {
        return relationPlayers;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getThreads() {
        return threads;
    }

    public static class dataConfigGeneratorMapping {
        private String columnName;
        private String[] columnNames;
        private String generator;
        private String listSeparator;
        private String matchByAttribute;
        private String[] matchByPlayers;

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
    }
}
