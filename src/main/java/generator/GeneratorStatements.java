package generator;

import graql.lang.pattern.variable.ThingVariable;

import java.util.ArrayList;

//    {
//        "matchInserts": [
//            "match": [],
//            "insert": String
//        ],
//        "inserts": []
//    }

//ArrayList<ThingVariable<?>>

public class GeneratorStatements {
    private ArrayList<MatchInsert> matchInserts = new ArrayList<>();
    private ArrayList<ThingVariable<?>> inserts = new ArrayList<>();

    public ArrayList<MatchInsert> getMatchInserts() {
        return matchInserts;
    }

    public void setMatchInserts(ArrayList<MatchInsert> matchInserts) {
        this.matchInserts = matchInserts;
    }

    public ArrayList<ThingVariable<?>> getInserts() {
        return inserts;
    }

    public void setInserts(ArrayList<ThingVariable<?>> inserts) {
        this.inserts = inserts;
    }

    public static class MatchInsert {
        private ArrayList<ThingVariable<?>> matches;
        private ThingVariable<?> insert;

        public MatchInsert(ArrayList<ThingVariable<?>> matches, ThingVariable<?> insert) {
            this.matches = matches;
            this.insert = insert;
        }

        public ArrayList<ThingVariable<?>> getMatches() {
            return matches;
        }

        public void setMatches(ArrayList<ThingVariable<?>> matches) {
            this.matches = matches;
        }

        public ThingVariable<?> getInsert() {
            return insert;
        }

        public void setInsert(ThingVariable<?> insert) {
            this.insert = insert;
        }
    }
}
