package generator;

import graql.lang.pattern.variable.ThingVariable;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class InsertGenerator {
    public ArrayList<ThingVariable<?>> graknEntityInsert(ArrayList<String> rows, String header) { return null; };
    public HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> graknRelationInsert(ArrayList<String> rows, String header) throws Exception { return null; };
    public HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> graknAppendAttributeInsert(ArrayList<String> rows, String header) throws Exception { return null; };
}
