package processor;

import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import configuration.ConfigEntryData;
import configuration.ConfigEntryProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;

import static processor.ProcessorUtil.*;

public class EntityInsertProcessor implements InsertProcessor {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private final ConfigEntryData dce;
    private final ConfigEntryProcessor pce;
    private final int dataPathIndex;

    public EntityInsertProcessor(ConfigEntryData dce,
                                 ConfigEntryProcessor pce,
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
                insertQueries.getDirectInserts().add(generateInsertQueries(row, header, rowCounter + batchCount));
            } catch (Exception e) {
                e.printStackTrace();
            }
            batchCount = batchCount + 1;
        }
        return insertQueries;
    }

    public ThingVariable<?> generateInsertQueries(String row,
                                                  String header,
                                                  int rowCounter) throws Exception {
        String[] rowTokens = tokenizeCSVStandard(row, dce.getSeparator());
        String[] columnNames = tokenizeCSVStandard(header, dce.getSeparator());
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        malformedRow(row, rowTokens, columnNames.length);

        ThingVariable.Thing directInsertStatement = generateDirectInsertStatementForThing(rowTokens, columnNames, rowCounter, dce, pce);

        if (isValidDirectInsert(directInsertStatement, pce)) {
            appLogger.debug("valid query: <insert " + directInsertStatement + ";>");
            return directInsertStatement;
        } else {
            dataLogger.warn("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: skipped row " + rowCounter + " b/c does not have a proper <isa> statement or is missing required attributes. Faulty tokenized row: " + Arrays.toString(rowTokens));
            return null;
        }
    }
}
