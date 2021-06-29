package processor;

import com.vaticle.typeql.lang.pattern.variable.ThingVariable;

import java.util.ArrayList;

public class InsertQueries {
    private final ArrayList<MatchInsert> matchInserts;
    private final ArrayList<ThingVariable<?>> directInserts;

    public InsertQueries() {
        this.matchInserts = new ArrayList<>();
        this.directInserts = new ArrayList<>();
    }

    public ArrayList<MatchInsert> getMatchInserts() {
        return matchInserts;
    }

    public ArrayList<ThingVariable<?>> getDirectInserts() {
        return directInserts;
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
