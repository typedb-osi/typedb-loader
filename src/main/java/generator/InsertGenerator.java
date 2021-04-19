package generator;

import graql.lang.pattern.variable.ThingVariable;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class InsertGenerator {
    public GeneratorStatements graknEntityInsert(ArrayList<String> rows, String header, int rowCounter) {
        return null;
    }

    public GeneratorStatements graknAttributeInsert(ArrayList<String> rows, String header, int rowCounter) {
        return null;
    }

    public HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> graknRelationInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception {
        return null;
    }

    public HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> graknAppendAttributeInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception {
        return null;
    }

}
