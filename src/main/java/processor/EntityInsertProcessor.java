package processor;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.pattern.variable.ThingVariable.Thing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static processor.ProcessorUtil.*;

public class EntityInsertProcessor implements InsertProcessor {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private final DataConfigEntry dce;
    private final ProcessorConfigEntry pce;
    private final int dataPathIndex;

    public EntityInsertProcessor(DataConfigEntry dce,
                                 ProcessorConfigEntry pce,
                                 int dataPathIndex) {
        super();
        this.dce = dce;
        this.pce = pce;
        this.dataPathIndex = dataPathIndex;
        appLogger.debug("Creating EntityInsertGenerator for processor " + pce.getProcessor() + " of type " + pce.getProcessorType());
    }

    public InsertQueries typeDBInsert(ArrayList<String> rows,
                                      String header,
                                      int rowCounter) throws IllegalArgumentException {
        InsertQueries insertQueries = new InsertQueries();
        int batchCount = 1;
        for (String row : rows) {
            try {
                insertQueries.getDirectInserts().add(graknEntityQueryFromRow(row, header, rowCounter + batchCount));
            } catch (Exception e) {
                e.printStackTrace();
            }
            batchCount = batchCount + 1;
        }
        return insertQueries;
    }

    public ThingVariable<?> graknEntityQueryFromRow(String row,
                                                    String header,
                                                    int rowCounter) throws Exception {
        String[] rowTokens = tokenizeCSVStandard(row, dce.getSeparator());
        String[] columnNames = tokenizeCSVStandard(header, dce.getSeparator());
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        malformedRow(row, rowTokens, columnNames.length);

        Thing directInsertStatement = generateDirectInsertStatementForThing(rowTokens, columnNames, rowCounter, dce, pce);

        if (isValidDirectInsert(directInsertStatement, pce)) {
            appLogger.debug("valid query: <insert " + directInsertStatement + ";>");
            return directInsertStatement;
        } else {
            dataLogger.warn("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: skipped row " + rowCounter + " b/c does not have a proper <isa> statement or is missing required attributes. Faulty tokenized row: " + Arrays.toString(rowTokens));
            return null;
        }
    }
}
