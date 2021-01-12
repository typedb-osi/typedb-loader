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
    private final ProcessorConfigEntry gce;
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");

    public EntityInsertGenerator(DataConfigEntry dataConfigEntry, ProcessorConfigEntry processorConfigEntry) {
        super();
        this.dce = dataConfigEntry;
        this.gce = processorConfigEntry;
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
        String[] tokens = row.split(dce.getSeparator());
        String[] headerTokens = header.split(dce.getSeparator());
        appLogger.debug("processing tokenized row: " + Arrays.toString(tokens));
        malformedRow(row, tokens, headerTokens.length);

        StatementInstance pattern = addEntity(insertCounter);

        for (DataConfigEntry.dataConfigGeneratorMapping dataAttribute : dce.getAttributes()) {
            pattern = addAttribute(tokens, pattern, headerTokens, dataAttribute, gce.getAttributeGenerator(dataAttribute.getGenerator()));
        }

        if (isValid(pattern)) {
            appLogger.debug("valid query: <" + pattern.toString() + ">");
            return pattern;
        } else {
            dataLogger.warn("in datapath <" + dce.getDataPath() + ">: skipped row b/c does not have a proper <isa> statement or is missing required attributes. Faulty tokenized row: " + Arrays.toString(tokens));
            return null;
        }
    }

    private StatementInstance addEntity(int insertCounter) {
        if (gce.getSchemaType() != null) {
            return Graql.var("e-" + insertCounter).isa(gce.getSchemaType());
        } else {
            throw new IllegalArgumentException("Required field <schemaType> not set in processor " + gce.getProcessor());
        }
    }

    private boolean isValid(StatementInstance si) {
        String statement = si.toString();
        if (!statement.contains("isa " + gce.getSchemaType())) {
            return false;
        }
        for (Map.Entry<String, ProcessorConfigEntry.ConceptGenerator> con : gce.getRequiredAttributes().entrySet()) {
            if (!statement.contains("has " + con.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }
}
