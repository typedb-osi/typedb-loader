package processor;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.constraint.ThingConstraint;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.pattern.variable.ThingVariable.Attribute;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;

import static processor.ProcessorUtil.*;

public class AttributeInsertProcessor extends InsertProcessor {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private final DataConfigEntry dce;
    private final ProcessorConfigEntry pce;
    private final int dataPathIndex;

    public AttributeInsertProcessor(DataConfigEntry dataConfigEntry, ProcessorConfigEntry processorConfigEntry, int dataPathIndex) {
        super();
        this.dce = dataConfigEntry;
        this.pce = processorConfigEntry;
        this.dataPathIndex = dataPathIndex;
        appLogger.debug("Creating AttributeInsertGenerator for processor " + processorConfigEntry.getProcessor() + " of type " + processorConfigEntry.getProcessorType());
    }

    public ProcessorStatement graknAttributeInsert(ArrayList<String> rows,
                                                   String header, int rowCounter) throws IllegalArgumentException {
        ProcessorStatement statements = new ProcessorStatement();
        int batchCount = 1;
        for (String row : rows) {
            try {
                ThingVariable<?> temp = graknAttributeQueryFromRow(row, header, rowCounter + batchCount);
                statements.getInserts().add(temp);
            } catch (Exception e) {
                e.printStackTrace();
            }
            batchCount = batchCount + 1;
        }
        return statements;
    }

    public ThingVariable<?> graknAttributeQueryFromRow(String row,
                                                       String header,
                                                       int rowCounter) throws Exception {
        String[] rowTokens = tokenizeCSVStandard(row, dce.getSeparator());
        String[] columnNames = tokenizeCSVStandard(header, dce.getSeparator());
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        malformedRow(row, rowTokens, columnNames.length);

        if (dce.getAttributes().length > 1) {
            //TODO: need to move into validation
            appLogger.error("the dataconfig entry for inserting independent attribute <" + pce.getSchemaType() + "> lists more than one column - this is not allowed");
            return null;
        } else {
            Attribute attributeInsertStatement = null;
            for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAttribute : dce.getAttributes()) {
                for (ThingConstraint.Value<?> valueConstraint : generateValueConstraints(rowTokens, columnNames, rowCounter, generatorMappingForAttribute, pce, generatorMappingForAttribute.getPreprocessor())) {
                    attributeInsertStatement = Graql.var("a").constrain(valueConstraint);
                }
            }
            if (attributeInsertStatement != null) {
                attributeInsertStatement.isa(pce.getSchemaType());
                if (isValid(attributeInsertStatement)) {
                    appLogger.debug("valid query: <insert " + attributeInsertStatement + ";>");
                    return attributeInsertStatement;
                } else {
                    dataLogger.warn("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: skipped row " + rowCounter + " b/c does not have a proper <isa> statement or is missing required attributes. Faulty tokenized row: " + Arrays.toString(rowTokens));
                    return null;
                }
            } else {
                return null;
            }

        }
    }

    private boolean isValid(Pattern pa) {
        String patternAsString = pa.toString();
        return patternAsString.contains("isa " + pce.getSchemaType());
    }
}
