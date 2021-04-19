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

import static generator.GeneratorUtil.addAttribute;
import static generator.GeneratorUtil.malformedRow;

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

    public GeneratorStatements graknAppendAttributeInsert(ArrayList<String> rows,
                                                          String header, int rowCounter) throws Exception {
        GeneratorStatements generatorStatements = new GeneratorStatements();

        int insertCounter = 0;
        int batchCounter = 1;
        for (String row : rows) {
            GeneratorStatements.MatchInsert tmp = graknAppendAttributeQueryFromRow(row, header, rowCounter + batchCounter);
            if (tmp != null) {
                if (tmp.getMatches() != null && tmp.getInsert() != null) {
                    generatorStatements.getMatchInserts().add(tmp);
                }
            }
            batchCounter = batchCounter + 1;
            insertCounter = insertCounter + 1;
        }
        return generatorStatements;
    }

    public GeneratorStatements.MatchInsert graknAppendAttributeQueryFromRow(String row,
                                                                            String header,
                                                                            int rowCounter) throws Exception {
        String fileSeparator = dce.getSeparator();
        String[] rowTokens = row.split(fileSeparator);
        String[] columnNames = header.split(fileSeparator);
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        malformedRow(row, rowTokens, columnNames.length);

        //TODO: move into config validation
        if (!validateDataConfigEntry()) {
            throw new IllegalArgumentException("data config entry for " + dce.getDataPath()[dataPathIndex] + " is incomplete - it needs at least one attribute used for matching (\"match\": true) and at least one attribute to be appended (\"match\": false or not set at all");
        }

        ArrayList<ThingVariable<?>> matchPatterns = new ArrayList<>();

        // get all attributes that are isMatch() --> construct match clause
        Thing appendAttributeMatchPattern = addEntityToMatchPattern();
        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForMatchAttribute : dce.getAttributes()) {
            if (generatorMappingForMatchAttribute.isMatch()) {
                appendAttributeMatchPattern = addAttribute(rowTokens, appendAttributeMatchPattern, columnNames, rowCounter, generatorMappingForMatchAttribute, pce, generatorMappingForMatchAttribute.getPreprocessor());
            }
        }
        matchPatterns.add(appendAttributeMatchPattern);

        // get all attributes that are !isMatch() --> construct insert clause
        UnboundVariable thingVar = addEntityToInsertPattern();
        Thing appendAttributeInsertPattern = null;
        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAppendAttribute : dce.getAttributes()) {
            if (!generatorMappingForAppendAttribute.isMatch()) {
                appendAttributeInsertPattern = addAttribute(rowTokens, thingVar, rowCounter, columnNames, generatorMappingForAppendAttribute, pce, generatorMappingForAppendAttribute.getPreprocessor());
            }
        }

        if (isValid(matchPatterns, appendAttributeInsertPattern)) {
            appLogger.debug("valid query: <" + assembleQuery(matchPatterns, appendAttributeInsertPattern) + ">");
            return new GeneratorStatements.MatchInsert(matchPatterns, appendAttributeInsertPattern);
        } else {
            dataLogger.warn("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: skipped row " + rowCounter + " b/c does not contain at least one match attribute and one insert attribute. Faulty tokenized row: " + Arrays.toString(rowTokens));
            return null;
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

    private Thing addEntityToMatchPattern() {
        if (pce.getSchemaType() != null) {
            return Graql.var("e").isa(pce.getSchemaType());
        } else {
            throw new IllegalArgumentException("Required field <schemaType> not set in processor " + pce.getProcessor());
        }
    }

    private UnboundVariable addEntityToInsertPattern() {
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
        ret.append(insertStatement.toString());
        return ret.toString();
    }

    private boolean isValid(ArrayList<ThingVariable<?>> matchStatements, ThingVariable<?> insertStatement) {

        if (insertStatement == null) {
            return false;
        }

        StringBuilder matchStatement = new StringBuilder();
        for (Pattern st : matchStatements) {
            matchStatement.append(st.toString());
        }

        // missing match attribute
        for (DataConfigEntry.DataConfigGeneratorMapping attributeMapping : dce.getMatchAttributes()) {
            String generatorKey = attributeMapping.getGenerator();
            ProcessorConfigEntry.ConceptGenerator generatorEntry = pce.getAttributeGenerator(generatorKey);
            if (!matchStatement.toString().contains("has " + generatorEntry.getAttributeType())) {
                return false;
            }
        }
        // missing required insert attribute
        for (Map.Entry<String, ProcessorConfigEntry.ConceptGenerator> generatorEntry : pce.getRequiredAttributes().entrySet()) {
            if (!insertStatement.toString().contains("has " + generatorEntry.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }
}
