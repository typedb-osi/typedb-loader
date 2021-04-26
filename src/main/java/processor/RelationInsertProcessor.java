package processor;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.constraint.ThingConstraint;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.pattern.variable.ThingVariable.Attribute;
import graql.lang.pattern.variable.ThingVariable.Relation;
import graql.lang.pattern.variable.ThingVariable.Thing;
import graql.lang.pattern.variable.UnboundVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static processor.ProcessorUtil.*;

public class RelationInsertProcessor implements InsertProcessor {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    public final DataConfigEntry dce;
    public final ProcessorConfigEntry pce;
    private final int dataPathIndex;

    public RelationInsertProcessor(DataConfigEntry dce, ProcessorConfigEntry processorConfigEntry, int dataPathIndex) {
        super();
        this.dce = dce;
        this.pce = processorConfigEntry;
        this.dataPathIndex = dataPathIndex;
        appLogger.debug("Creating RelationInsertGenerator for " + pce.getProcessor() + " of type " + pce.getProcessorType());
    }

    public InsertQueries typeDBInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception {
        InsertQueries insertQueries = new InsertQueries();
        int batchCounter = 1;
        for (String row : rows) {
            InsertQueries.MatchInsert tmp = generateInsertQueries(row, header, rowCounter + batchCounter);
            insertQueries.getMatchInserts().add(tmp);
            batchCounter = batchCounter + 1;
        }
        return insertQueries;
    }

    public InsertQueries.MatchInsert generateInsertQueries(String row, String header, int rowCounter) throws Exception {
        String[] rowTokens = tokenizeCSVStandard(row, dce.getSeparator());
        String[] columnNames = tokenizeCSVStandard(header, dce.getSeparator());
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        ProcessorUtil.malformedRow(row, rowTokens, columnNames.length);

        ArrayList<ThingVariable<?>> miStatements = new ArrayList<>(createPlayerMatchAndInsert(rowTokens, columnNames, rowCounter));

        if (miStatements.size() >= 1) {
            ArrayList<ThingVariable<?>> matchStatements = new ArrayList<>(miStatements.subList(0, miStatements.size() - 1));

            if (!matchStatements.isEmpty()) {
                ThingVariable<?> playersInsertStatement = miStatements.subList(miStatements.size() - 1, miStatements.size()).get(0);
                Relation assembledInsertStatement = relationInsert((Relation) playersInsertStatement);

                if (dce.getAttributeProcessorMappings() != null) {
                    for (DataConfigEntry.ConceptProcessorMapping generatorMappingForAttribute : dce.getAttributeProcessorMappings()) {
                        for (ThingConstraint.Has hasConstraint : generateHasConstraint(rowTokens, columnNames, rowCounter, generatorMappingForAttribute, pce)) {
                            assembledInsertStatement.constrain(hasConstraint);
                        }
                    }
                }

                if (isValid(matchStatements, assembledInsertStatement)) {
                    appLogger.debug("valid query: <" + assembleQuery(matchStatements, assembledInsertStatement) + ">");
                    return new InsertQueries.MatchInsert(matchStatements, assembledInsertStatement);
                } else {
                    dataLogger.warn("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: skipped row " + rowCounter + " b/c does not have a proper <isa> statement or is missing required players or attributes. Faulty tokenized row: " + Arrays.toString(rowTokens));
                    return new InsertQueries.MatchInsert(null, null);
                }
            } else {
                dataLogger.warn("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: skipped row " + rowCounter + " b/c has 0 players. Faulty tokenized row: " + Arrays.toString(rowTokens));
                return new InsertQueries.MatchInsert(null, null);
            }
        } else {
            dataLogger.warn("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: skipped row " + rowCounter + " b/c empty.");
            return new InsertQueries.MatchInsert(null, null);
        }
    }

    private String assembleQuery(ArrayList<ThingVariable<?>> matchStatements,
                                 ThingVariable<?> insertStatement) {
        StringBuilder ret = new StringBuilder();
        ret.append("match ");
        for (Pattern st : matchStatements) {
            ret.append(st.toString()).append("; ");
        }
        ret.append("write ");
        ret.append(insertStatement.toString());
        ret.append(";");
        return ret.toString();
    }

    private ArrayList<ThingVariable<?>> createPlayerMatchAndInsert(String[] rowTokens,
                                                                   String[] columnNames,
                                                                   int rowCounter) {
        ArrayList<ThingVariable<?>> matches = new ArrayList<>();
        UnboundVariable relVariable = Graql.var("rel");
        ArrayList<ArrayList<String>> relationStrings = new ArrayList<>();
        int playerCounter = 0;

        // add Entity Players:
        for (DataConfigEntry.ConceptProcessorMapping cpm : dce.getPlayerProcessorMappings()) {
            ProcessorConfigEntry.ConceptProcessor cp = pce.getPlayerGenerator(cpm.getConceptProcessor());
            int columnNameIndex = idxOf(columnNames, cpm.getColumnName());

            if (columnNameIndex == -1) {
                appLogger.error("The column header " + cpm.getColumnName() + " specified in your dataconfig cannot be found in the file you specified.");
            }

            if (rowTokens.length > columnNameIndex && // make sure that there are enough rowTokens in the row for your column of interest
                    !cleanToken(rowTokens[columnNameIndex]).isEmpty()) { // make sure that after cleaning, there is more than an empty string
                String cleanedRecord = cleanToken(rowTokens[columnNameIndex]);
                String cls = cpm.getListSeparator();
                if (cls != null) {
                    for (String exploded : cleanedRecord.split(cls)) {
                        if (!cleanToken(exploded).isEmpty()) {
                            String explodedCleanedValue = cleanToken(exploded);
                            String pVar = cp.getPlayerType() + "-" + playerCounter;
                            String pRole = cp.getRoleType();
                            if (cp.getUniquePlayerId().contains("_attribute_player_")) {
                                //TODO now
                                matches.add(createAttributePlayerMatchStatement(explodedCleanedValue, rowCounter, cp, pVar, cpm.getPreprocessor()));
                            } else {
                                //TODO next
                                matches.add(createEntityPlayerMatchStatement(explodedCleanedValue, rowCounter, cp, pVar, cpm.getPreprocessor()));
                            }
                            ArrayList<String> rel = new ArrayList<>();
                            rel.add(pRole);
                            rel.add(pVar);
                            relationStrings.add(rel);
                            playerCounter++;
                        }
                    }
                } else { // single player, no columnListSeparator
                    String playerVariable = cp.getPlayerType() + "-" + playerCounter;
                    String playerRole = cp.getRoleType();
                    if (cp.getUniquePlayerId().contains("_attribute_player_")) {
                        matches.add(createAttributePlayerMatchStatement(cleanedRecord, rowCounter, cp, playerVariable, cpm.getPreprocessor()));
                    } else {
                        matches.add(createEntityPlayerMatchStatement(cleanedRecord, rowCounter, cp, playerVariable, cpm.getPreprocessor()));
                    }
                    ArrayList<String> rel = new ArrayList<>();
                    rel.add(playerRole);
                    rel.add(playerVariable);
                    relationStrings.add(rel);
                    playerCounter++;
                }
            }
        }
        // add Relation Players
        if (dce.getRelationPlayerProcessorMappings() != null) {
            for (DataConfigEntry.ConceptProcessorMapping generatorMappingForRelationPlayer : dce.getRelationPlayerProcessorMappings()) {
                String generatorKey = generatorMappingForRelationPlayer.getConceptProcessor();
                ProcessorConfigEntry.ConceptProcessor playerGenerator = pce.getRelationPlayerGenerator(generatorKey);

                // if matching RelationPlayer by Attribute:
                if (generatorMappingForRelationPlayer.getMatchByAttribute() != null) {
                    String columnName = generatorMappingForRelationPlayer.getColumnName();
                    int columnNameIndex = idxOf(columnNames, columnName);

                    if (columnNameIndex == -1) {
                        appLogger.error("The column header " + generatorMappingForRelationPlayer.getColumnName() + " specified in your dataconfig cannot be found in the file you specified.");
                    }

                    if (rowTokens.length > columnNameIndex &&
                            !cleanToken(rowTokens[columnNameIndex]).isEmpty()) {
                        String currentCleanedToken = cleanToken(rowTokens[columnNameIndex]);
                        String columnListSeparator = generatorMappingForRelationPlayer.getListSeparator();
                        if (columnListSeparator != null) {
                            for (String exploded : currentCleanedToken.split(columnListSeparator)) {
                                if (!cleanToken(exploded).isEmpty()) {
                                    String currentExplodedCleanedToken = cleanToken(exploded);
//                                    String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter + "-" + insertCounter;
                                    String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter;
                                    String playerRole = playerGenerator.getRoleType();
                                    matches.add(createRelationPlayerMatchStatementByAttribute(currentExplodedCleanedToken, rowCounter, playerGenerator, generatorMappingForRelationPlayer, playerVariable));
                                    ArrayList<String> rel = new ArrayList<>();
                                    rel.add(playerRole);
                                    rel.add(playerVariable);
                                    relationStrings.add(rel);
                                    playerCounter++;
                                }
                            }
                        } else { // single player, no listSeparator
//                            String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter + "-" + insertCounter;
                            String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter;
                            String playerRole = playerGenerator.getRoleType();
                            matches.add(createRelationPlayerMatchStatementByAttribute(currentCleanedToken, rowCounter, playerGenerator, generatorMappingForRelationPlayer, playerVariable));
                            ArrayList<String> rel = new ArrayList<>();
                            rel.add(playerRole);
                            rel.add(playerVariable);
                            relationStrings.add(rel);
                            playerCounter++;
                        }
                    }
                    // if matching the relationStrings player by players in that relationStrings:
                } else if (generatorMappingForRelationPlayer.getMatchByPlayers().length > 0) {
                    int[] columnNameIndices = indicesOf(columnNames, generatorMappingForRelationPlayer.getColumnNames());

                    for (int i : columnNameIndices) {
                        if (i == -1) {
                            appLogger.error("The column header " + generatorMappingForRelationPlayer.getColumnName() + " specified in your dataconfig cannot be found in the file you specified.");
                        }
                    }

                    if (Arrays.stream(columnNameIndices).max().isPresent()) {
                        int maxColumnIndex = Arrays.stream(columnNameIndices).max().getAsInt();
                        if (rowTokens.length > maxColumnIndex) {
                            String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter;
                            String playerRole = playerGenerator.getRoleType();
                            matches.addAll(createRelationPlayerMatchStatementByPlayers(rowTokens, rowCounter, columnNameIndices, playerGenerator, generatorMappingForRelationPlayer, playerVariable));
                            ArrayList<String> rel = new ArrayList<>();
                            rel.add(playerRole);
                            rel.add(playerVariable);
                            relationStrings.add(rel);
                            playerCounter++;
                        }
                    } else {
                        appLogger.error("ColumnNameIndices are empty - this should NEVER happen, please report bug on github...");
                    }
                } else {
                    appLogger.error("Your config entry for column header: " + generatorMappingForRelationPlayer.getColumnName() + "needs to specify matching either by player/s or by an attribute");
                }
            }
        }

        if (relationStrings.size() >= 1) {
            Relation returnRelation = relVariable.rel(relationStrings.get(0).get(0), relationStrings.get(0).get(1));
            for (ArrayList<String> rel : relationStrings.subList(1, relationStrings.size())) {
                returnRelation = returnRelation.rel(rel.get(0), rel.get(1));
            }
            matches.add(returnRelation);
        }

        return matches;
    }

    private ThingVariable<?> createEntityPlayerMatchStatement(String cleanedToken,
                                                              int rowCounter,
                                                              ProcessorConfigEntry.ConceptProcessor playerGenerator,
                                                              String playerVariable,
                                                              DataConfigEntry.ConceptProcessorMapping.PreprocessorConfig preprocessorConfig) {
        Thing ms = Graql
                .var(playerVariable)
                .isa(playerGenerator.getPlayerType());
        String attributeType = playerGenerator.getUniquePlayerId();
        AttributeValueType attributeValueType = playerGenerator.getIdValueType();
        ms = ms.constrain(valueToHasConstraint(attributeType, generateValueConstraint(attributeType, attributeValueType, cleanedToken, rowCounter, preprocessorConfig)));
        return ms;
    }

    private ThingVariable<?> createAttributePlayerMatchStatement(String cleanedToken,
                                                                 int rowCounter,
                                                                 ProcessorConfigEntry.ConceptProcessor playerGenerator,
                                                                 String playerVariable,
                                                                 DataConfigEntry.ConceptProcessorMapping.PreprocessorConfig preprocessorConfig) {
        UnboundVariable uv = Graql.var(playerVariable);
        Attribute ms = uv.constrain(generateValueConstraint(playerGenerator.getUniquePlayerId(), playerGenerator.getIdValueType(), cleanedToken, rowCounter, preprocessorConfig));
        ms = ms.isa(playerGenerator.getPlayerType());
        return ms;
    }

    private ThingVariable<?> createRelationPlayerMatchStatementByAttribute(String cleanedToken,
                                                                           int rowCounter,
                                                                           ProcessorConfigEntry.ConceptProcessor playerGenerator,
                                                                           DataConfigEntry.ConceptProcessorMapping dcm,
                                                                           String playerVariable) {
        Thing ms = Graql
                .var(playerVariable)
                .isa(playerGenerator.getPlayerType());
        String attributeType = playerGenerator.getMatchByAttribute().get(dcm.getMatchByAttribute()).getAttributeType();
        AttributeValueType attributeValueType = playerGenerator.getMatchByAttribute().get(dcm.getMatchByAttribute()).getValueType();
        ms = ms.constrain(valueToHasConstraint(attributeType, generateValueConstraint(attributeType, attributeValueType, cleanedToken, rowCounter, dcm.getPreprocessor())));
        return ms;
    }

    private ArrayList<ThingVariable<?>> createRelationPlayerMatchStatementByPlayers(String[] rowTokens, int rowCounter, int[] columnNameIndices, ProcessorConfigEntry.ConceptProcessor playerGenerator, DataConfigEntry.ConceptProcessorMapping dcm, String playerVariable) {
        ArrayList<ThingVariable<?>> assembledMatchStatements = new ArrayList<>();
        UnboundVariable relVariable = Graql.var(playerVariable);
        ArrayList<ArrayList<String>> relationStrings = new ArrayList<>();

        //match the n entites with their attributes
        int i = 0;
        for (int columnNameIndex : columnNameIndices) {
            if (!cleanToken(rowTokens[columnNameIndex]).isEmpty()) {
                String cleanedToken = cleanToken(rowTokens[columnNameIndex]);
                String relationPlayerPlayerVariable = "relplayer-player-" + i;
                String relationPlayerPlayerType = playerGenerator.getMatchByPlayer().get(dcm.getMatchByPlayers()[i]).getPlayerType();
                String relationPlayerPlayerAttributeType = playerGenerator.getMatchByPlayer().get(dcm.getMatchByPlayers()[i]).getUniquePlayerId();
                AttributeValueType relationPlayerPlayerAttributeValueType = playerGenerator.getMatchByPlayer().get(dcm.getMatchByPlayers()[i]).getIdValueType();

                Thing relationPlayerCurrentPlayerMatchStatement = Graql.var(relationPlayerPlayerVariable).isa(relationPlayerPlayerType);
                relationPlayerCurrentPlayerMatchStatement = relationPlayerCurrentPlayerMatchStatement.constrain(valueToHasConstraint(relationPlayerPlayerAttributeType, generateValueConstraint(relationPlayerPlayerAttributeType, relationPlayerPlayerAttributeValueType, cleanedToken, rowCounter, dcm.getPreprocessor())));
                assembledMatchStatements.add(relationPlayerCurrentPlayerMatchStatement);

                // here add the matched player to the relation statement (i.e.: (role: $variable)):
                String relationPlayerPlayerRole = playerGenerator.getMatchByPlayer().get(dcm.getMatchByPlayers()[i]).getRoleType();
                ArrayList<String> rel = new ArrayList<>();
                rel.add(relationPlayerPlayerRole);
                rel.add(relationPlayerPlayerVariable);
                relationStrings.add(rel);
                i++;
            } else {
                // this ensures that only relations in which all required players are present actually enter the match statement - empty list = skip of write
                boolean requiredButNotPresent = playerGenerator.getMatchByPlayer().get(dcm.getMatchByPlayers()[i]).isRequired();
                if (requiredButNotPresent) {
                    return new ArrayList<>();
                }
            }
        }

        if (relationStrings.size() >= 1 && assembledMatchStatements.size() > 0) {
            Relation returnRelation = relVariable.rel(relationStrings.get(0).get(0), relationStrings.get(0).get(1));
            for (ArrayList<String> rel : relationStrings.subList(1, relationStrings.size())) {
                returnRelation = returnRelation.rel(rel.get(0), rel.get(1));
            }
            assembledMatchStatements.add(returnRelation.isa(playerGenerator.getPlayerType()));
        }

        return assembledMatchStatements;
    }


    private Relation relationInsert(Relation si) {
        if (si != null) {
            return si.isa(pce.getSchemaType());
        } else {
            return null;
        }
    }

    private boolean isValid(ArrayList<ThingVariable<?>> matchStatements, ThingVariable<?> insertStatement) {
        StringBuilder matchStatement = new StringBuilder();
        for (Pattern st : matchStatements) {
            matchStatement.append(st.toString());
        }
        // missing required players
        for (Map.Entry<String, ProcessorConfigEntry.ConceptProcessor> generatorEntry : pce.getRelationRequiredPlayers().entrySet()) {
            if (!matchStatement.toString().contains("isa " + generatorEntry.getValue().getPlayerType())) {
                return false;
            }
            if (!insertStatement.toString().contains(generatorEntry.getValue().getRoleType())) {
                return false;
            }
        }
        // missing required attribute
        for (Map.Entry<String, ProcessorConfigEntry.ConceptProcessor> generatorEntry : pce.getRequiredAttributes().entrySet()) {
            if (!insertStatement.toString().contains("has " + generatorEntry.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }
}