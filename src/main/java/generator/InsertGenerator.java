package generator;

import graql.lang.statement.Statement;

import java.util.ArrayList;

public abstract class InsertGenerator {
    public ArrayList<Statement> graknEntityInsert(ArrayList<String> rows, String header) { return null; };
    public ArrayList<ArrayList<ArrayList<Statement>>> graknRelationInsert(ArrayList<String> rows, String header) throws Exception { return null; };
    public ArrayList<ArrayList<ArrayList<Statement>>> graknAppendAttributeInsert(ArrayList<String> rows, String header) throws Exception { return null; };
}
