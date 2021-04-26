package processor;

import java.util.ArrayList;

public interface InsertProcessor {
    ProcessorStatement typeDBInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception;
}
