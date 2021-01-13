package generator;

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

import static generator.GeneratorUtil.*;

public class AppendAttributeGenerator extends InsertGenerator {

    private final DataConfigEntry dce;
    private final ProcessorConfigEntry pce;
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");

    public AppendAttributeGenerator(DataConfigEntry dataConfigEntry, ProcessorConfigEntry processorConfigEntry) {
        super();
        this.dce = dataConfigEntry;
        this.pce = processorConfigEntry;
        appLogger.debug("Creating AppendAttribute for processor " + processorConfigEntry.getProcessor() + " of type " + processorConfigEntry.getProcessorType());
    }

    public ArrayList<ArrayList<ArrayList<Statement>>> graknAppendAttributeInsert(ArrayList<String> rows, String header) throws Exception {
        ArrayList<ArrayList<ArrayList<Statement>>> matchInsertStatements = new ArrayList<>();

        ArrayList<ArrayList<Statement>> matchStatements = new ArrayList<>();
        ArrayList<ArrayList<Statement>> insertStatements = new ArrayList<>();

        int insertCounter = 0;

        for (String row : rows) {
            ArrayList<ArrayList<Statement>> tmp = graknAppendAttributeQueryFromRow(row, header, insertCounter);
            if (tmp != null) {
                if (tmp.get(0) != null && tmp.get(1) != null) {
                    matchStatements.add(tmp.get(0));
                    insertStatements.add(tmp.get(1));
                    insertCounter++;
                }
            }

        }
        matchInsertStatements.add(matchStatements);
        matchInsertStatements.add(insertStatements);
        return matchInsertStatements;
    }

    public ArrayList<ArrayList<Statement>> graknAppendAttributeQueryFromRow(String row, String header, int insertCounter) throws Exception {
        String fileSeparator = dce.getSeparator();
        String[] rowTokens = row.split(fileSeparator);
        String[] columnNames = header.split(fileSeparator);
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        malformedRow(row, rowTokens, columnNames.length);

        if (!validateDataConfigEntry()) {
            throw new IllegalArgumentException("data config entry for " + dce.getDataPath() + " is incomplete - it needs at least one attribute used for matching (\"match\": true) and at least one attribute to be appended (\"match\": false or not set at all");
        }

        ArrayList<Statement> matchStatements = new ArrayList<>();
        ArrayList<Statement> insertStatements = new ArrayList<>();

        // get all attributes that are isMatch() --> construct match clause
        StatementInstance appendAttributeMatchStatement = addEntityToMatchStatement(insertCounter);
        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForMatchAttribute : dce.getAttributes()) {
            if (generatorMappingForMatchAttribute.isMatch()){
                appendAttributeMatchStatement = addAttribute(rowTokens, appendAttributeMatchStatement, columnNames, generatorMappingForMatchAttribute, pce);
            }
        }
        matchStatements.add(appendAttributeMatchStatement);

        // get all attributes that are !isMatch() --> construct insert clause
        Statement appendAttributeInsertStatement = addEntityToInsertStatement(insertCounter);
        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAppendAttribute : dce.getAttributes()) {
            if (!generatorMappingForAppendAttribute.isMatch()){
                appendAttributeInsertStatement = addAttribute(rowTokens, appendAttributeMatchStatement, columnNames, generatorMappingForAppendAttribute, pce);
            }
        }
        insertStatements.add((StatementInstance) appendAttributeInsertStatement);

        ArrayList<ArrayList<Statement>> assembledStatements = new ArrayList<>();
        assembledStatements.add(matchStatements);
        assembledStatements.add(insertStatements);


        if (isValid(assembledStatements)) {
            appLogger.debug("valid query: <" + assembleQuery(assembledStatements).toString() + ">");
            return assembledStatements;
        } else {
            dataLogger.warn("in datapath <" + dce.getDataPath() + ">: skipped row b/c does not contain at least one match attribute and one insert attribute. Faulty tokenized row: " + Arrays.toString(rowTokens));
            return null;
        }
    }

    private boolean validateDataConfigEntry() {
        boolean containsMatchAttribute = false;
        boolean containsAppendAttribute = false;
        for (DataConfigEntry.DataConfigGeneratorMapping attributeMapping : dce.getAttributes()) {
            if (attributeMapping.isMatch()){
                containsMatchAttribute = true;
            }
            if (!attributeMapping.isMatch()) {
                containsAppendAttribute = true;
            }
        }
        return containsMatchAttribute && containsAppendAttribute;
    }

    private StatementInstance addEntityToMatchStatement(int insertCounter) {
        if (pce.getSchemaType() != null) {
            return Graql.var("e-" + insertCounter).isa(pce.getSchemaType());
        } else {
            throw new IllegalArgumentException("Required field <schemaType> not set in processor " + pce.getProcessor());
        }
    }

    private Statement addEntityToInsertStatement(int insertCounter) {
        if (pce.getSchemaType() != null) {
            return Graql.var("e-" + insertCounter);
        } else {
            throw new IllegalArgumentException("Required field <schemaType> not set in processor " + pce.getProcessor());
        }
    }

    private String assembleQuery(ArrayList<ArrayList<Statement>> queries) {
        StringBuilder ret = new StringBuilder();
        for (Statement st : queries.get(0)) {
            ret.append(st.toString());
        }
        ret.append(queries.get(1).get(0).toString());
        return ret.toString();
    }

    private boolean isValid(ArrayList<ArrayList<Statement>> si) {
        ArrayList<Statement> matchStatements = si.get(0);
        ArrayList<Statement> insertStatements = si.get(1);
        StringBuilder matchStatement = new StringBuilder();
        for (Statement st:matchStatements) {
            matchStatement.append(st.toString());
        }
        String insertStatement = insertStatements.get(0).toString();

        // missing match attribute
        for (DataConfigEntry.DataConfigGeneratorMapping attributeMapping: dce.getMatchAttributes()) {
            String generatorKey = attributeMapping.getGenerator();
            ProcessorConfigEntry.ConceptGenerator generatorEntry = pce.getAttributeGenerator(generatorKey);
            if (!matchStatement.toString().contains("has " + generatorEntry.getAttributeType())) {
                return false;
            }
        }
        // missing required insert attribute
        for (Map.Entry<String, ProcessorConfigEntry.ConceptGenerator> generatorEntry: pce.getRequiredAttributes().entrySet()) {
            if (!insertStatement.contains("has " + generatorEntry.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }
}
