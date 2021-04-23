package processor;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.pattern.variable.ThingVariable.Thing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static processor.ProcessorUtil.*;

public class EntityInsertProcessor extends InsertProcessor {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private final DataConfigEntry dce;
    private final ProcessorConfigEntry pce;
    private final int dataPathIndex;

    public EntityInsertProcessor(DataConfigEntry dataConfigEntry, ProcessorConfigEntry processorConfigEntry, int dataPathIndex) {
        super();
        this.dce = dataConfigEntry;
        this.pce = processorConfigEntry;
        this.dataPathIndex = dataPathIndex;
        appLogger.debug("Creating EntityInsertGenerator for processor " + processorConfigEntry.getProcessor() + " of type " + processorConfigEntry.getProcessorType());
    }

    public ProcessorStatement graknEntityInsert(ArrayList<String> rows,
                                                String header,
                                                int rowCounter) throws IllegalArgumentException {
        ProcessorStatement processorStatement = new ProcessorStatement();
        int batchCount = 1;
        for (String row : rows) {
            try {
                ThingVariable<?> temp = graknEntityQueryFromRow(row, header, rowCounter + batchCount);
                processorStatement.getInserts().add(temp);
            } catch (Exception e) {
                e.printStackTrace();
            }
            batchCount = batchCount + 1;
        }
        return processorStatement;
    }

    public ThingVariable<?> graknEntityQueryFromRow(String row,
                                                    String header,
                                                    int rowCounter) throws Exception {
        String[] rowTokens = tokenizeCSVStandard(row, dce.getSeparator());
        String[] columnNames = tokenizeCSVStandard(header, dce.getSeparator());
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        malformedRow(row, rowTokens, columnNames.length);

        Thing entityInsertStatement = addEntityToStatement();

        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAttribute : dce.getAttributes()) {
            entityInsertStatement = addAttribute(rowTokens, entityInsertStatement, columnNames, rowCounter, generatorMappingForAttribute, pce, generatorMappingForAttribute.getPreprocessor());
        }

        if (isValid(entityInsertStatement)) {
            appLogger.debug("valid query: <insert " + entityInsertStatement + ";>");
            return entityInsertStatement;
        } else {
            dataLogger.warn("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: skipped row " + rowCounter + " b/c does not have a proper <isa> statement or is missing required attributes. Faulty tokenized row: " + Arrays.toString(rowTokens));
            return null;
        }
    }

    private Thing addEntityToStatement() {
        if (pce.getSchemaType() != null) {
            return Graql.var("e").isa(pce.getSchemaType());
        } else {
            throw new IllegalArgumentException("Required field <schemaType> not set in processor " + pce.getProcessor());
        }
    }

    private boolean isValid(Pattern pa) {
        String patternAsString = pa.toString();
        if (!patternAsString.contains("isa " + pce.getSchemaType())) {
            return false;
        }
        for (Map.Entry<String, ProcessorConfigEntry.ConceptGenerator> con : pce.getRequiredAttributes().entrySet()) {
            if (!patternAsString.contains("has " + con.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }
}
