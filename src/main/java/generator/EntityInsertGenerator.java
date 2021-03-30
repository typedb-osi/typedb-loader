package generator;

import static generator.GeneratorUtil.malformedRow;
import static generator.GeneratorUtil.addAttribute;

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

public class EntityInsertGenerator extends InsertGenerator {

    private final DataConfigEntry dce;
    private final ProcessorConfigEntry pce;
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");

    public EntityInsertGenerator(DataConfigEntry dataConfigEntry, ProcessorConfigEntry processorConfigEntry) {
        super();
        this.dce = dataConfigEntry;
        this.pce = processorConfigEntry;
        appLogger.debug("Creating EntityInsertGenerator for processor " + processorConfigEntry.getProcessor() + " of type " + processorConfigEntry.getProcessorType());
    }

    public ArrayList<ThingVariable<?>> graknEntityInsert(ArrayList<String> rows,
                                                      String header) throws IllegalArgumentException {
        ArrayList<ThingVariable<?>> patterns = new ArrayList<>();
        for (String row : rows) {
            try {
                ThingVariable temp = graknEntityQueryFromRow(row, header);
                if (temp != null) {
                    patterns.add(temp);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return patterns;
    }

    public ThingVariable graknEntityQueryFromRow(String row,
                                                 String header) throws Exception {
        String fileSeparator = dce.getSeparator();
        String[] rowTokens = row.split(fileSeparator);
        String[] columnNames = header.split(fileSeparator);
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        malformedRow(row, rowTokens, columnNames.length);

        Thing entityInsertStatement = addEntityToStatement();

        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAttribute : dce.getAttributes()) {
            entityInsertStatement = addAttribute(rowTokens, entityInsertStatement, columnNames, generatorMappingForAttribute, pce, generatorMappingForAttribute.getPreprocessor());
        }

        if (isValid(entityInsertStatement)) {
            appLogger.debug("valid query: <insert " + entityInsertStatement.toString() + ";>");
            return entityInsertStatement;
        } else {
            dataLogger.warn("in datapath <" + dce.getDataPath() + ">: skipped row b/c does not have a proper <isa> statement or is missing required attributes. Faulty tokenized row: " + Arrays.toString(rowTokens));
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
