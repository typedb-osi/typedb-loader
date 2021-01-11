package configuration;

public class DataConfigEntry {
    private String dataPath;
    private String separator;
    private String processor;
    private GeneratorSpecification[] attributes;
    private GeneratorSpecification[] players;
    private GeneratorSpecification[] relationPlayers;
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

    public GeneratorSpecification[] getAttributes() {
        return attributes;
    }

    public GeneratorSpecification[] getPlayers() {
        return players;
    }

    public GeneratorSpecification[] getRelationPlayers() {
        return relationPlayers;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getThreads() {
        return threads;
    }

    public static class GeneratorSpecification {
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
