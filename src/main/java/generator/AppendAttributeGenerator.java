package generator;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.pattern.variable.ThingVariable.Thing;
import graql.lang.pattern.variable.UnboundVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static generator.GeneratorUtil.*;

public class AppendAttributeGenerator extends InsertGenerator {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private final DataConfigEntry dce;
    private final ProcessorConfigEntry pce;
    private final int dataPathIndex;

    public AppendAttributeGenerator(DataConfigEntry dataConfigEntry,
                                    ProcessorConfigEntry processorConfigEntry,
                                    int dataPathIndex) {
        super();
        this.dce = dataConfigEntry;
        this.pce = processorConfigEntry;
        this.dataPathIndex = dataPathIndex;
        appLogger.debug("Creating AppendAttribute for processor " + processorConfigEntry.getProcessor() + " of type " + processorConfigEntry.getProcessorType());
    }

    public GeneratorStatement graknAppendAttributeInsert(ArrayList<String> rows,
                                                         String header, int rowCounter) throws Exception {
        GeneratorStatement generatorStatement = new GeneratorStatement();

        int batchCounter = 1;
        for (String row : rows) {
            GeneratorStatement.MatchInsert tmp = graknAppendAttributeQueryFromRow(row, header, rowCounter + batchCounter);
            generatorStatement.getMatchInserts().add(tmp);
            batchCounter = batchCounter + 1;
        }
        return generatorStatement;
    }

    public GeneratorStatement.MatchInsert graknAppendAttributeQueryFromRow(String row,
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

        ArrayList<ThingVariable<?>> matchStatements = new ArrayList<>();

        // get all attributes that are isMatch() --> construct match clause
        Thing matchStatement = addEntityToMatch();
        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForMatchAttribute : dce.getAttributes()) {
            if (generatorMappingForMatchAttribute.isMatch()) {
                matchStatement = addAttribute(rowTokens, matchStatement, columnNames, rowCounter, generatorMappingForMatchAttribute, pce, generatorMappingForMatchAttribute.getPreprocessor());
            }
        }
        matchStatements.add(matchStatement);

        // get all attributes that are !isMatch() --> construct insert clause
        UnboundVariable tmpMI = addEntityToInsert();
        Thing matchInsertStatement = null;
        boolean first = true;
        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAppendAttribute : dce.getAttributes()) {
            if (!generatorMappingForAppendAttribute.isMatch()) {
                if (first) {
                    matchInsertStatement = addAttribute(rowTokens, tmpMI, rowCounter, columnNames, generatorMappingForAppendAttribute, pce, generatorMappingForAppendAttribute.getPreprocessor());
                    if (matchInsertStatement != null) {
                        first = false;
                    }
                } else {
                    matchInsertStatement = addAttribute(rowTokens, matchInsertStatement, columnNames, rowCounter, generatorMappingForAppendAttribute, pce, generatorMappingForAppendAttribute.getPreprocessor());
                }
            }
        }

        if (isValid(matchStatements, matchInsertStatement)) {
            appLogger.debug("valid match-insert query: <" + assembleQuery(matchStatements, matchInsertStatement) + ">");
            return new GeneratorStatement.MatchInsert(matchStatements, matchInsertStatement);
        } else {
            dataLogger.trace("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: row " + rowCounter
                    + "  does not contain at least one match attribute and one insert attribute. Tokenized row: " + Arrays.toString(rowTokens)
                    + " - invalid query: " + assembleQuery(matchStatements, matchInsertStatement) + " - will insert without doing match first");
            return new GeneratorStatement.MatchInsert(null, null);
        }
    }

    private boolean validateDataConfigEntry() {
        boolean containsMatchAttribute = false;
        boolean containsAppendAttribute = false;
        for (DataConfigEntry.DataConfigGeneratorMapping attributeMapping : dce.getAttributes()) {
            if (attributeMapping.isMatch()) {
                containsMatchAttribute = true;
            }
            if (!attributeMapping.isMatch()) {
                containsAppendAttribute = true;
            }
        }
        return containsMatchAttribute && containsAppendAttribute;
    }

    private Thing addEntityToMatch() {
        if (pce.getSchemaType() != null) {
            return Graql.var("e").isa(pce.getSchemaType());
        } else {
            throw new IllegalArgumentException("Required field <schemaType> not set in processor " + pce.getProcessor());
        }
    }

    private UnboundVariable addEntityToInsert() {
        if (pce.getSchemaType() != null) {
            return Graql.var("e");
        } else {
            throw new IllegalArgumentException("Required field <schemaType> not set in processor " + pce.getProcessor());
        }
    }

    private String assembleQuery(ArrayList<ThingVariable<?>> matchStatements, ThingVariable<?> insertStatement) {
        StringBuilder ret = new StringBuilder();
        for (ThingVariable<?> st : matchStatements) {
            ret.append(st.toString());
        }
        if (insertStatement != null) ret.append(insertStatement.toString()); else ret.append("matchInsertStatement is null");
        return ret.toString();
    }

    private boolean isValid(ArrayList<ThingVariable<?>> matchStatements, ThingVariable<?> matchInsertInsertStatement) {

        if (matchInsertInsertStatement == null) return false;
        StringBuilder matchStatement = new StringBuilder();
        for (Pattern st : matchStatements) {
            matchStatement.append(st.toString());
        }
        ArrayList<String> matchAttributes = new ArrayList<>();
        for (DataConfigEntry.DataConfigGeneratorMapping attributeMapping : dce.getMatchAttributes()) {
            String generatorKey = attributeMapping.getGenerator();
            ProcessorConfigEntry.ConceptGenerator generatorEntry = pce.getAttributeGenerator(generatorKey);
            if (!matchStatement.toString().contains("has " + generatorEntry.getAttributeType())) {
                return false;
            }
            matchAttributes.add(generatorEntry.getAttributeType());
        }
        for (Map.Entry<String, ProcessorConfigEntry.ConceptGenerator> generatorEntry : pce.getRequiredAttributes().entrySet()) {
            if (!matchInsertInsertStatement.toString().contains("has " + generatorEntry.getValue().getAttributeType()) &&
                    !matchAttributes.contains(generatorEntry.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }
}
