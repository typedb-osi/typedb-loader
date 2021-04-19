package generator;

import java.util.ArrayList;

public abstract class InsertGenerator {
    public GeneratorStatements graknEntityInsert(ArrayList<String> rows, String header, int rowCounter) {
        return null;
    }

    public GeneratorStatements graknAttributeInsert(ArrayList<String> rows, String header, int rowCounter) {
        return null;
    }

    public GeneratorStatements graknRelationInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception {
        return null;
    }

    public GeneratorStatements graknAppendAttributeInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception {
        return null;
    }

    public GeneratorStatements graknAppendOrInsertInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception {
        return null;
    }

}
