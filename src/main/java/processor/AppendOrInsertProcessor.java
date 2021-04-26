package processor;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.constraint.ThingConstraint;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.pattern.variable.ThingVariable.Thing;
import graql.lang.pattern.variable.UnboundVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static processor.ProcessorUtil.*;

public class AppendOrInsertProcessor extends InsertProcessor {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private final DataConfigEntry dce;
    private final ProcessorConfigEntry pce;
    private final int dataPathIndex;

    public AppendOrInsertProcessor(DataConfigEntry dataConfigEntry,
                                   ProcessorConfigEntry processorConfigEntry,
                                   int dataPathIndex) {
        super();
        this.dce = dataConfigEntry;
        this.pce = processorConfigEntry;
        this.dataPathIndex = dataPathIndex;
        appLogger.debug("Creating AppendOrInsertGenerator for processor " + processorConfigEntry.getProcessor() + " of type " + processorConfigEntry.getProcessorType());
    }

    public ProcessorStatement graknAppendOrInsertInsert(ArrayList<String> rows,
                                                        String header, int rowCounter) throws Exception {
        ProcessorStatement processorStatement = new ProcessorStatement();

        int batchCounter = 1;
        for (String row : rows) {
            graknAppendAttributeQueryFromRow(processorStatement, row, header, rowCounter + batchCounter);
            batchCounter = batchCounter + 1;
        }
        return processorStatement;
    }

    public void graknAppendAttributeQueryFromRow(ProcessorStatement processorStatement,
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

        // get all attributes that are isMatch() --> construct match clause
        ArrayList<ThingVariable<?>> matchStatements = new ArrayList<>();
        Thing matchStatement = addEntityToMatchPattern();
        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForMatchAttribute : dce.getAttributes()) {
            if (generatorMappingForMatchAttribute.isMatch()) {
//                matchStatement = addAttribute(rowTokens, matchStatement, columnNames, rowCounter, generatorMappingForMatchAttribute, pce, generatorMappingForMatchAttribute.getPreprocessor());
                for (ThingConstraint.Has hasConstraint : generateHasConstraint(rowTokens, columnNames, rowCounter, generatorMappingForMatchAttribute, pce, generatorMappingForMatchAttribute.getPreprocessor())) {
                    matchStatement.constrain(hasConstraint);
                }
            }
        }
        matchStatements.add(matchStatement);

        // get all attributes that are !isMatch() --> construct insert clause
        UnboundVariable unboundMatchInsertStatement = addUnboundVarToInsertPattern();
        ThingVariable<?> matchInsertStatement = null;
        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAppendAttribute : dce.getAttributes()) {
            if (!generatorMappingForAppendAttribute.isMatch()) {
//                    matchInsertStatement = addAttribute(rowTokens, tmpMI, rowCounter, columnNames, generatorMappingForAppendAttribute, pce, generatorMappingForAppendAttribute.getPreprocessor());
                for (ThingConstraint.Has hasConstraint : generateHasConstraint(rowTokens, columnNames, rowCounter, generatorMappingForAppendAttribute, pce, generatorMappingForAppendAttribute.getPreprocessor())) {
                    if (matchInsertStatement == null) {
                        matchInsertStatement = unboundMatchInsertStatement.constrain(hasConstraint);
                    } else {
                        matchInsertStatement.constrain(hasConstraint);
                    }
                }
            }
        }

        // construct insertStatement
        Thing insertStatement = addEntityToMatchPattern();
        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAppendAttribute : dce.getAttributes()) {
//            insertStatement = addAttribute(rowTokens, insertStatement, columnNames, rowCounter, generatorMappingForAppendAttribute, pce, generatorMappingForAppendAttribute.getPreprocessor());
            for (ThingConstraint.Has hasConstraint : generateHasConstraint(rowTokens, columnNames, rowCounter, generatorMappingForAppendAttribute, pce, generatorMappingForAppendAttribute.getPreprocessor())) {
                insertStatement.constrain(hasConstraint);
            }
        }

        if (isValid(matchStatements, matchInsertStatement)) {
            appLogger.debug("valid match-insert query: <" + assembleQuery(matchStatements, matchInsertStatement) + ">");
            processorStatement.getMatchInserts().add(new ProcessorStatement.MatchInsert(matchStatements, matchInsertStatement));
        } else {
            dataLogger.trace("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: row " + rowCounter
                    + "  does not contain at least one match attribute and one insert attribute. Tokenized row: " + Arrays.toString(rowTokens)
                    + " - invalid query: " + assembleQuery(matchStatements, matchInsertStatement) + " - will insert without doing match first");
            processorStatement.getMatchInserts().add(new ProcessorStatement.MatchInsert(null, null));
        }

        if (isValid(insertStatement)) {
            appLogger.debug("valid insert query: <" + insertStatement + ">");
            processorStatement.getInserts().add(insertStatement);
        } else {
            dataLogger.warn("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: row " + rowCounter
                    + " does not contain at least one match attribute and one insert attribute. Skipping faulty tokenized row: " + Arrays.toString(rowTokens)
                    + " - invalid query: " + insertStatement.toString());
            processorStatement.getInserts().add(null);
        }
    }

    private boolean validateDataConfigEntry() {
        for (DataConfigEntry.DataConfigGeneratorMapping attributeMapping : dce.getAttributes()) {
            if (attributeMapping.isMatch()) {
                return true;
            }
        }
        return false;
    }

    private Thing addEntityToMatchPattern() {
        if (pce.getSchemaType() != null) {
            return Graql.var("e").isa(pce.getSchemaType());
        } else {
            throw new IllegalArgumentException("Required field <schemaType> not set in processor " + pce.getProcessor());
        }
    }

    private UnboundVariable addUnboundVarToInsertPattern() {
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

    private boolean isValid(ArrayList<ThingVariable<?>> matchStatements, ThingVariable<?> matchInsertInsertStatement) {

        // Match-Insert pairs:
        // missing match attribute
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
        // missing required insert attribute
        for (Map.Entry<String, ProcessorConfigEntry.ConceptGenerator> generatorEntry : pce.getRequiredAttributes().entrySet()) {
            if (!matchInsertInsertStatement.toString().contains("has " + generatorEntry.getValue().getAttributeType()) &&
                    !matchAttributes.contains(generatorEntry.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }

    private boolean isValid(ThingVariable<?> insertStatement) {
        // missing required insert attribute
        if (insertStatement == null) return false;
        for (Map.Entry<String, ProcessorConfigEntry.ConceptGenerator> generatorEntry : pce.getRequiredAttributes().entrySet()) {
            if (!insertStatement.toString().contains("has " + generatorEntry.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }


}
