package processor;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import graql.lang.pattern.variable.ThingVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;

import static processor.ProcessorUtil.*;

public class AppendAttributeProcessor implements InsertProcessor {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private final DataConfigEntry dce;
    private final ProcessorConfigEntry pce;
    private final int dataPathIndex;

    public AppendAttributeProcessor(DataConfigEntry dataConfigEntry,
                                    ProcessorConfigEntry processorConfigEntry,
                                    int dataPathIndex) {
        super();
        this.dce = dataConfigEntry;
        this.pce = processorConfigEntry;
        this.dataPathIndex = dataPathIndex;
        appLogger.debug("Creating AppendAttribute for processor " + processorConfigEntry.getProcessor() + " of type " + processorConfigEntry.getProcessorType());
    }

    public InsertQueries typeDBInsert(ArrayList<String> rows,
                                      String header,
                                      int rowCounter) throws Exception {
        InsertQueries insertQueries = new InsertQueries();
        int batchCounter = 1;
        for (String row : rows) {
            InsertQueries.MatchInsert tmp = generateInsertQueries(row, header, rowCounter + batchCounter);
            insertQueries.getMatchInserts().add(tmp);
            batchCounter = batchCounter + 1;
        }
        return insertQueries;
    }

    public InsertQueries.MatchInsert generateInsertQueries(String row,
                                                           String header,
                                                           int rowCounter) throws Exception {
        String[] rowTokens = tokenizeCSVStandard(row, dce.getSeparator());
        String[] columnNames = tokenizeCSVStandard(header, dce.getSeparator());
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        malformedRow(row, rowTokens, columnNames.length);

        //TODO: move into config validation
        if (!validateDataConfigEntry()) {
            throw new IllegalArgumentException("data config entry for " + dce.getDataPath()[dataPathIndex] + " is incomplete - it needs at least one attribute used for matching (\"match\": true) and at least one attribute to be appended (\"match\": false or not set at all");
        }

        ArrayList<ThingVariable<?>> matchStatements = generateMatchStatementsFromMatchAttributes(rowTokens, columnNames, rowCounter, dce, pce);
        ThingVariable<?> matchInsertStatement = generateInsertStatementsWithoutMatchAttributes(rowTokens, columnNames, rowCounter, dce, pce);

        if (isValidMatchAppend(matchStatements, matchInsertStatement, dce, pce)) {
            appLogger.debug("valid match-insert query: <" + matchInsertQueriesToString(matchStatements, matchInsertStatement) + ">");
            return new InsertQueries.MatchInsert(matchStatements, matchInsertStatement);
        } else {
            dataLogger.trace("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: row " + rowCounter
                    + "  does not contain at least one match attribute and one insert attribute. Tokenized row: " + Arrays.toString(rowTokens)
                    + " - invalid query: " + matchInsertQueriesToString(matchStatements, matchInsertStatement) + " - will insert without doing match first");
            return new InsertQueries.MatchInsert(null, null);
        }
    }

    private boolean validateDataConfigEntry() {
        boolean containsMatchAttribute = false;
        boolean containsAppendAttribute = false;
        for (DataConfigEntry.ConceptProcessorMapping attributeMapping : dce.getAttributeProcessorMappings()) {
            if (attributeMapping.isMatch()) {
                containsMatchAttribute = true;
            }
            if (!attributeMapping.isMatch()) {
                containsAppendAttribute = true;
            }
        }
        return containsMatchAttribute && containsAppendAttribute;
    }

}
