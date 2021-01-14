package generator;

import static generator.GeneratorUtil.malformedRow;
import static generator.GeneratorUtil.addAttribute;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import graql.lang.Graql;
import graql.lang.statement.Statement;
import graql.lang.statement.StatementInstance;
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

    public ArrayList<Statement> graknEntityInsert(ArrayList<String> rows, String header) throws IllegalArgumentException {
        ArrayList<Statement> statements = new ArrayList<>();
        int insertCounter = 0;
        for (String row : rows) {
            try {
                Statement temp = graknEntityQueryFromRow(row, header, insertCounter);
                if (temp != null) {
                    statements.add(temp);
                }
                insertCounter++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return statements;
    }

    public StatementInstance graknEntityQueryFromRow(String row, String header, int insertCounter) throws Exception {
        String fileSeparator = dce.getSeparator();
        String[] rowTokens = row.split(fileSeparator);
        String[] columnNames = header.split(fileSeparator);
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        malformedRow(row, rowTokens, columnNames.length);

        StatementInstance entityInsertStatement = addEntityToStatement(insertCounter);

        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAttribute : dce.getAttributes()) {
            entityInsertStatement = addAttribute(rowTokens, entityInsertStatement, columnNames, generatorMappingForAttribute, pce, generatorMappingForAttribute.getPreprocessor());
        }

        if (isValid(entityInsertStatement)) {
            appLogger.debug("valid query: <" + entityInsertStatement.toString() + ">");
            return entityInsertStatement;
        } else {
            dataLogger.warn("in datapath <" + dce.getDataPath() + ">: skipped row b/c does not have a proper <isa> statement or is missing required attributes. Faulty tokenized row: " + Arrays.toString(rowTokens));
            return null;
        }
    }

    private StatementInstance addEntityToStatement(int insertCounter) {
        if (pce.getSchemaType() != null) {
            return Graql.var("e-" + insertCounter).isa(pce.getSchemaType());
        } else {
            throw new IllegalArgumentException("Required field <schemaType> not set in processor " + pce.getProcessor());
        }
    }

    private boolean isValid(StatementInstance si) {
        String statement = si.toString();
        if (!statement.contains("isa " + pce.getSchemaType())) {
            return false;
        }
        for (Map.Entry<String, ProcessorConfigEntry.ConceptGenerator> con : pce.getRequiredAttributes().entrySet()) {
            if (!statement.contains("has " + con.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }
}
