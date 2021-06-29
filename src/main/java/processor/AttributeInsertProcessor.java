package processor;

import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.constraint.ThingConstraint;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import configuration.ConfigEntryData;
import configuration.ConfigEntryProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;

import static processor.ProcessorUtil.*;

public class AttributeInsertProcessor implements InsertProcessor {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private final ConfigEntryData dce;
    private final ConfigEntryProcessor pce;
    private final int dataPathIndex;

    public AttributeInsertProcessor(ConfigEntryData dce,
                                    ConfigEntryProcessor pce,
                                    int dataPathIndex) {
        super();
        this.dce = dce;
        this.pce = pce;
        this.dataPathIndex = dataPathIndex;
        appLogger.debug("Creating AttributeInsertGenerator for processor " + pce.getProcessor() + " of type " + pce.getProcessorType());
    }

    public InsertQueries typeDBInsert(ArrayList<String> rows,
                                      String header,
                                      int rowCounter) throws IllegalArgumentException {
        InsertQueries statements = new InsertQueries();
        int batchCount = 1;
        for (String row : rows) {
            try {
                statements.getDirectInserts().add(generateInsertQueries(row, header, rowCounter + batchCount));
            } catch (Exception e) {
                e.printStackTrace();
            }
            batchCount = batchCount + 1;
        }
        return statements;
    }

    public ThingVariable<?> generateInsertQueries(String row,
                                                  String header,
                                                  int rowCounter) throws Exception {
        //TODO use CSVFormat datastructure
        String[] rowTokens = tokenizeCSVStandard(row, dce.getSeparator());
        String[] columnNames = tokenizeCSVStandard(header, dce.getSeparator());
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        malformedRow(row, rowTokens, columnNames.length);

        if (dce.getAttributeProcessorMappings().length > 1) {
            //TODO: need to move into validation
            appLogger.error("the dataconfig entry for inserting independent attribute <" + pce.getSchemaType() + "> lists more than one column - this is not allowed");
            return null;
        } else {
            ThingVariable.Attribute attributeInsertStatement = null;
            for (ConfigEntryData.ConceptProcessorMapping cpm : dce.getAttributeProcessorMappings()) {
                //TODO: check that no list sep in this attributeGeneratorMapping in validation - because otherwise statement produced will be invalid
                for (ThingConstraint.Value<?> valueConstraint : generateValueConstraints(rowTokens, columnNames, rowCounter, cpm, pce)) {
                    attributeInsertStatement = TypeQL.var("a").constrain(valueConstraint);
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

    private boolean isValid(Pattern insertStatement) {
        String patternAsString = insertStatement.toString();
        return patternAsString.contains("isa " + pce.getSchemaType());
    }
}
