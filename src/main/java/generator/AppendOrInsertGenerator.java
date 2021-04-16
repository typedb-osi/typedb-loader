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
import java.util.HashMap;
import java.util.Map;

import static generator.GeneratorUtil.addAttribute;
import static generator.GeneratorUtil.malformedRow;

public class AppendOrInsertGenerator extends InsertGenerator {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private final DataConfigEntry dce;
    private final ProcessorConfigEntry pce;
    private final int dataPathIndex;

    public AppendOrInsertGenerator(DataConfigEntry dataConfigEntry,
                                   ProcessorConfigEntry processorConfigEntry,
                                   int dataPathIndex) {
        super();
        this.dce = dataConfigEntry;
        this.pce = processorConfigEntry;
        this.dataPathIndex = dataPathIndex;
        appLogger.debug("Creating AppendOrInsertGenerator for processor " + processorConfigEntry.getProcessor() + " of type " + processorConfigEntry.getProcessorType());
    }

    public HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> graknAppendAttributeInsert(ArrayList<String> rows,
                                                                                              String header, int rowCounter) throws Exception {
        HashMap<String, ArrayList<ArrayList<ThingVariable<?>>>> matchInsertPatterns = new HashMap<>();

        ArrayList<ArrayList<ThingVariable<?>>> matchPatterns = new ArrayList<>();
        ArrayList<ArrayList<ThingVariable<?>>> insertPatterns = new ArrayList<>();

        int insertCounter = 0;
        int batchCounter = 1;
        for (String row : rows) {

            ArrayList<ArrayList<ThingVariable<?>>> tmp = graknAppendAttributeQueryFromRow(row, header, insertCounter, rowCounter + batchCounter);
            if (tmp != null) {
                if (tmp.get(0) != null && tmp.get(1) != null) {
                    matchPatterns.add(tmp.get(0));
                    insertPatterns.add(tmp.get(1));
                    insertCounter++;
                }
            }
            batchCounter = batchCounter + 1;
        }
        matchInsertPatterns.put("match", matchPatterns);
        matchInsertPatterns.put("insert", insertPatterns);
        return matchInsertPatterns;
    }

    public ArrayList<ArrayList<ThingVariable<?>>> graknAppendAttributeQueryFromRow(String row,
                                                                                   String header,
                                                                                   int insertCounter,
                                                                                   int rowCounter) throws Exception {
        String fileSeparator = dce.getSeparator();
        String[] rowTokens = row.split(fileSeparator);
        String[] columnNames = header.split(fileSeparator);
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        malformedRow(row, rowTokens, columnNames.length);

        //TODO: move this into configValidation
        if (!validateDataConfigEntry()) {
            throw new IllegalArgumentException("data config entry for " + dce.getDataPath()[dataPathIndex] + " is incomplete - it needs at least one attribute used for matching (\"match\": true) and at least one attribute to be appended (\"match\": false or not set at all");
        }

        ArrayList<ThingVariable<?>> matchPatterns = new ArrayList<>();
        ArrayList<ThingVariable<?>> insertPatterns = new ArrayList<>();

        //TODO overall: make two hashmaps, one containing match-inserts, one containing only inserts, then flow is: run through match-inserts, if found and insert is OK, do next; if insert gives no result/no match-insert present because row didn't have value, use pure insert statement at same index and insert, then commit together

        // get all attributes that are isMatch() --> construct match clause
        //TODO: modify so that if the row is empty, the match clause is dropped for the index
        //TODO: note - already takes in multiple match attributes, non?
        Thing appendAttributeMatchPattern = addEntityToMatchPattern(insertCounter);
        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForMatchAttribute : dce.getAttributes()) {
            if (generatorMappingForMatchAttribute.isMatch()) {
                appendAttributeMatchPattern = addAttribute(rowTokens, appendAttributeMatchPattern, columnNames, rowCounter, generatorMappingForMatchAttribute, pce, generatorMappingForMatchAttribute.getPreprocessor());
            }
        }
        matchPatterns.add(appendAttributeMatchPattern);

        // get all attributes that are !isMatch() --> construct insert clause
        // TODO: this will have to be reversed on fail to find --> then need to put these into
        UnboundVariable thingVar = addEntityToInsertPattern(insertCounter);
        Thing appendAttributeInsertPattern = null;
        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAppendAttribute : dce.getAttributes()) {
            if (!generatorMappingForAppendAttribute.isMatch()) {
                appendAttributeInsertPattern = addAttribute(rowTokens, thingVar, rowCounter, columnNames, generatorMappingForAppendAttribute, pce, generatorMappingForAppendAttribute.getPreprocessor());
            }
        }
        if (appendAttributeInsertPattern != null) {
            insertPatterns.add(appendAttributeInsertPattern);
        }

        ArrayList<ArrayList<ThingVariable<?>>> assembledPatterns = new ArrayList<>();
        assembledPatterns.add(matchPatterns);
        assembledPatterns.add(insertPatterns);


        if (isValid(assembledPatterns)) {
            appLogger.debug("valid query: <" + assembleQuery(assembledPatterns) + ">");
            return assembledPatterns;
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

    private Thing addEntityToMatchPattern(int insertCounter) {
        if (pce.getSchemaType() != null) {
            return Graql.var("e").isa(pce.getSchemaType());
        } else {
            throw new IllegalArgumentException("Required field <schemaType> not set in processor " + pce.getProcessor());
        }
    }

    private UnboundVariable addEntityToInsertPattern(int insertCounter) {
        if (pce.getSchemaType() != null) {
            return Graql.var("e");
        } else {
            throw new IllegalArgumentException("Required field <schemaType> not set in processor " + pce.getProcessor());
        }
    }

    private String assembleQuery(ArrayList<ArrayList<ThingVariable<?>>> queries) {
        StringBuilder ret = new StringBuilder();
        for (ThingVariable<?> st : queries.get(0)) {
            ret.append(st.toString());
        }
        ret.append(queries.get(1).get(0).toString());
        return ret.toString();
    }

    private boolean isValid(ArrayList<ArrayList<ThingVariable<?>>> si) {
        ArrayList<ThingVariable<?>> matchPatterns = si.get(0);
        ArrayList<ThingVariable<?>> insertPatterns = si.get(1);

        if (insertPatterns.size() < 1) {
            return false;
        }

        StringBuilder matchPattern = new StringBuilder();
        for (Pattern st : matchPatterns) {
            matchPattern.append(st.toString());
        }
        String insertPattern = insertPatterns.get(0).toString();

        // missing match attribute
        for (DataConfigEntry.DataConfigGeneratorMapping attributeMapping : dce.getMatchAttributes()) {
            String generatorKey = attributeMapping.getGenerator();
            ProcessorConfigEntry.ConceptGenerator generatorEntry = pce.getAttributeGenerator(generatorKey);
            if (!matchPattern.toString().contains("has " + generatorEntry.getAttributeType())) {
                return false;
            }
        }
        // missing required insert attribute
        for (Map.Entry<String, ProcessorConfigEntry.ConceptGenerator> generatorEntry : pce.getRequiredAttributes().entrySet()) {
            if (!insertPattern.contains("has " + generatorEntry.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }
}
