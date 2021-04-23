package processor;

import graql.lang.pattern.variable.ThingVariable;

import java.util.ArrayList;

public class ProcessorStatement {
    private final ArrayList<MatchInsert> matchInserts;
    private final ArrayList<ThingVariable<?>> inserts;

    public ProcessorStatement() {
        this.matchInserts = new ArrayList<>();
        this.inserts = new ArrayList<>();
    }

    public ArrayList<MatchInsert> getMatchInserts() {
        return matchInserts;
    }

    public ArrayList<ThingVariable<?>> getInserts() {
        return inserts;
    }

    public static class MatchInsert {
        private final ArrayList<ThingVariable<?>> matches;
        private ThingVariable<?> insert;

        public MatchInsert(ArrayList<ThingVariable<?>> matches, ThingVariable<?> insert) {
            this.matches = matches;
            this.insert = insert;
        }

        public ArrayList<ThingVariable<?>> getMatches() {
            return matches;
        }

        public ThingVariable<?> getInsert() {
            return insert;
        }

        public void setInsert(ThingVariable<?> insert) {
            this.insert = insert;
        }
    }
}
