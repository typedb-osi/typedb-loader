package processor;

import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import configuration.ConfigEntryData;
import configuration.ConfigEntryProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;

import static processor.ProcessorUtil.*;

public class AppendOrInsertProcessor implements InsertProcessor {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private final ConfigEntryData dce;
    private final ConfigEntryProcessor pce;
    private final int dataPathIndex;

    public AppendOrInsertProcessor(ConfigEntryData configEntryData,
                                   ConfigEntryProcessor configEntryProcessor,
                                   int dataPathIndex) {
        super();
        this.dce = configEntryData;
        this.pce = configEntryProcessor;
        this.dataPathIndex = dataPathIndex;
        appLogger.debug("Creating AppendOrInsertGenerator for processor " + configEntryProcessor.getProcessor() + " of type " + configEntryProcessor.getProcessorType());
    }

    public InsertQueries typeDBInsert(ArrayList<String> rows,
                                      String header, int rowCounter) throws Exception {
        InsertQueries insertQueries = new InsertQueries();

        int batchCounter = 1;
        for (String row : rows) {
            generateInsertQueries(insertQueries, row, header, rowCounter + batchCounter);
            batchCounter = batchCounter + 1;
        }
        return insertQueries;
    }

    public void generateInsertQueries(InsertQueries insertQueries,
                                      String row,
                                      String header,
                                      int rowCounter) throws Exception {
        String[] rowTokens = tokenizeCSVStandard(row, dce.getSeparator());
        String[] columnNames = tokenizeCSVStandard(header, dce.getSeparator());
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        malformedRow(row, rowTokens, columnNames.length);

        //TODO: move this into configValidation
        if (!validateDataConfigEntry()) {
            throw new IllegalArgumentException("data config entry for " + dce.getDataPath()[dataPathIndex] + " is incomplete - it needs at least one attribute used for matching (\"match\": true)");
        }

        ArrayList<ThingVariable<?>> matchStatements = generateMatchStatementsFromMatchAttributes(rowTokens, columnNames, rowCounter, dce, pce);
        ThingVariable<?> matchInsertStatement = generateInsertStatementsWithoutMatchAttributes(rowTokens, columnNames, rowCounter, dce, pce);
        ThingVariable.Thing directInsertStatement = generateDirectInsertStatementForThing(rowTokens, columnNames, rowCounter, dce, pce);

        if (isValidMatchAppend(matchStatements, matchInsertStatement, dce, pce)) {
            appLogger.debug("valid match-insert query: <" + matchInsertQueriesToString(matchStatements, matchInsertStatement) + ">");
            insertQueries.getMatchInserts().add(new InsertQueries.MatchInsert(matchStatements, matchInsertStatement));
        } else {
            dataLogger.trace("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: row " + rowCounter
                    + "  does not contain at least one match attribute and one insert attribute. Tokenized row: " + Arrays.toString(rowTokens)
                    + " - invalid query: " + matchInsertQueriesToString(matchStatements, matchInsertStatement) + " - will insert without doing match first");
            insertQueries.getMatchInserts().add(new InsertQueries.MatchInsert(null, null));
        }

        if (isValidDirectInsert(directInsertStatement, pce)) {
            appLogger.debug("valid insert query: <" + directInsertStatement + ">");
            insertQueries.getDirectInserts().add(directInsertStatement);
        } else {
            dataLogger.warn("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: row " + rowCounter
                    + " does not contain at least one match attribute and one insert attribute. Skipping faulty tokenized row: " + Arrays.toString(rowTokens)
                    + " - invalid query: " + directInsertStatement.toString());
            insertQueries.getDirectInserts().add(null);
        }
    }

    private boolean validateDataConfigEntry() {
        for (ConfigEntryData.ConceptProcessorMapping attributeMapping : dce.getAttributeProcessorMappings()) {
            if (attributeMapping.isMatch()) {
                return true;
            }
        }
        return false;
    }

}
