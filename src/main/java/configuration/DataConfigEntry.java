package configuration;

public class DataConfigEntry {
    private String dataPath;
    private String sep;
    private String processor;
    private GenSpec[] attributes;
    private GenSpec[] players;
    private int batchSize;
    private int threads;

    public String getDataPath() {
        return dataPath;
    }

    public String getSep() {
        return sep;
    }

    public String getProcessor() {
        return processor;
    }

    public GenSpec[] getAttributes() {
        return attributes;
    }

    public GenSpec[] getPlayers() {
        return players;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getThreads() {
        return threads;
    }

    public static class GenSpec {
        private String columnName;
        private String generator;
        private String listSeparator;

        public String getColumnName() {
            return columnName;
        }

        public String getGenerator() {
            return generator;
        }

        public String getListSeparator() {
            return listSeparator;
        }
    }
}
