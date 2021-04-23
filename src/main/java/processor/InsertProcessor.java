package processor;

import java.util.ArrayList;

public abstract class InsertProcessor {
    public ProcessorStatement graknEntityInsert(ArrayList<String> rows, String header, int rowCounter) {
        return null;
    }

    public ProcessorStatement graknAttributeInsert(ArrayList<String> rows, String header, int rowCounter) {
        return null;
    }

    public ProcessorStatement graknRelationInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception {
        return null;
    }

    public ProcessorStatement graknAppendAttributeInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception {
        return null;
    }

    public ProcessorStatement graknAppendOrInsertInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception {
        return null;
    }

}
