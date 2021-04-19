package generator;

import java.util.ArrayList;

public abstract class InsertGenerator {
    public GeneratorStatement graknEntityInsert(ArrayList<String> rows, String header, int rowCounter) {
        return null;
    }

    public GeneratorStatement graknAttributeInsert(ArrayList<String> rows, String header, int rowCounter) {
        return null;
    }

    public GeneratorStatement graknRelationInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception {
        return null;
    }

    public GeneratorStatement graknAppendAttributeInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception {
        return null;
    }

    public GeneratorStatement graknAppendOrInsertInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception {
        return null;
    }

}
