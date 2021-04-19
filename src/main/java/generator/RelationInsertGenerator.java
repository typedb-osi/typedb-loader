package generator;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;
import graql.lang.pattern.variable.ThingVariable.Attribute;
import graql.lang.pattern.variable.ThingVariable.Relation;
import graql.lang.pattern.variable.ThingVariable.Thing;
import graql.lang.pattern.variable.UnboundVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static generator.GeneratorUtil.*;

public class RelationInsertGenerator extends InsertGenerator {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    public final DataConfigEntry dce;
    public final ProcessorConfigEntry pce;
    private final int dataPathIndex;

    public RelationInsertGenerator(DataConfigEntry dce, ProcessorConfigEntry processorConfigEntry, int dataPathIndex) {
        super();
        this.dce = dce;
        this.pce = processorConfigEntry;
        this.dataPathIndex = dataPathIndex;
        appLogger.debug("Creating RelationInsertGenerator for " + pce.getProcessor() + " of type " + pce.getProcessorType());
    }

    public GeneratorStatements graknRelationInsert(ArrayList<String> rows, String header, int rowCounter) throws Exception {
        GeneratorStatements generatorStatements = new GeneratorStatements();

        int batchCounter = 1;
        for (String row : rows) {
            GeneratorStatements.MatchInsert tmp = graknRelationshipQueryFromRow(row, header, rowCounter + batchCounter);
            if (tmp != null) {
                if (tmp.getMatches() != null && tmp.getInsert() != null) {
                    generatorStatements.getMatchInserts().add(tmp);
                }
            }
            batchCounter = batchCounter + 1;
        }
        return generatorStatements;
    }

    public GeneratorStatements.MatchInsert graknRelationshipQueryFromRow(String row, String header, int rowCounter) throws Exception {
        String fileSeparator = dce.getSeparator();
        String[] rowTokens = row.split(fileSeparator);
        String[] columnNames = header.split(fileSeparator);
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        GeneratorUtil.malformedRow(row, rowTokens, columnNames.length);

        ArrayList<ThingVariable<?>> miStatements = new ArrayList<>(createPlayerMatchAndInsert(rowTokens, columnNames, rowCounter));

        if (miStatements.size() >= 1) {
            ArrayList<ThingVariable<?>> matchStatements = new ArrayList<>(miStatements.subList(0, miStatements.size() - 1));

            if (!matchStatements.isEmpty()) {
                ThingVariable<?> playersInsertStatement = miStatements.subList(miStatements.size() - 1, miStatements.size()).get(0);
                Relation assembledInsertStatement = relationInsert((Relation) playersInsertStatement);

                if (dce.getAttributes() != null) {
                    for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForAttribute : dce.getAttributes()) {
                        assembledInsertStatement = addAttribute(rowTokens, assembledInsertStatement, columnNames, rowCounter, generatorMappingForAttribute, pce, generatorMappingForAttribute.getPreprocessor());
                    }
                }

                if (isValid(matchStatements, assembledInsertStatement)) {
                    appLogger.debug("valid query: <" + assembleQuery(matchStatements, assembledInsertStatement) + ">");
                    return new GeneratorStatements.MatchInsert(matchStatements, assembledInsertStatement);
                } else {
                    dataLogger.warn("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: skipped row " + rowCounter + " b/c does not have a proper <isa> statement or is missing required players or attributes. Faulty tokenized row: " + Arrays.toString(rowTokens));
                    return null;
                }
            } else {
                dataLogger.warn("in datapath <" + dce.getDataPath()[dataPathIndex] + ">: skipped row " + rowCounter + " b/c has 0 players. Faulty tokenized row: " + Arrays.toString(rowTokens));
                return null;
            }
        } else {
            return null;
        }
    }

    private String assembleQuery(ArrayList<ThingVariable<?>> matchStatements, ThingVariable<?> insertStatement) {
        StringBuilder ret = new StringBuilder();
        ret.append("match ");
        for (Pattern st : matchStatements) {
            ret.append(st.toString()).append("; ");
        }
        ret.append("insert ");
        ret.append(insertStatement.toString());
        ret.append(";");
        return ret.toString();
    }

    private Collection<? extends ThingVariable<?>> createPlayerMatchAndInsert(String[] rowTokens, String[] columnNames, int rowCounter) {
        ArrayList<ThingVariable<?>> players = new ArrayList<>();
        UnboundVariable relVariable = Graql.var("rel");
        ArrayList<ArrayList<String>> relationStrings = new ArrayList<>();
        int playerCounter = 0;

        // add Entity Players:
        for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForPlayer : dce.getPlayers()) {
            String generatorKey = generatorMappingForPlayer.getGenerator();
            ProcessorConfigEntry.ConceptGenerator playerGenerator = pce.getPlayerGenerator(generatorKey);
            String columnName = generatorMappingForPlayer.getColumnName();
            int columnNameIndex = idxOf(columnNames, columnName);

            if (columnNameIndex == -1) {
                appLogger.error("The column header " + generatorMappingForPlayer.getColumnName() + " specified in your dataconfig cannot be found in the file you specified.");
            }

            if (rowTokens.length > columnNameIndex && // make sure that there are enough rowTokens in the row for your column of interest
                    !cleanToken(rowTokens[columnNameIndex]).isEmpty()) { // make sure that after cleaning, there is more than an empty string
                String currentCleanedToken = cleanToken(rowTokens[columnNameIndex]);
                String columnListSeparator = generatorMappingForPlayer.getListSeparator();
                if (columnListSeparator != null) {
                    for (String exploded : currentCleanedToken.split(columnListSeparator)) {
                        if (!cleanToken(exploded).isEmpty()) {
                            String currentExplodedCleanedToken = cleanToken(exploded);
                            String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter;
                            String playerRole = playerGenerator.getRoleType();
                            if (playerGenerator.getUniquePlayerId().contains("_attribute_player_")) {
                                players.add(createAttributePlayerMatchStatement(currentExplodedCleanedToken, rowCounter, playerGenerator, playerVariable, generatorMappingForPlayer.getPreprocessor()));
                            } else {
                                players.add(createEntityPlayerMatchStatement(currentExplodedCleanedToken, rowCounter, playerGenerator, playerVariable, generatorMappingForPlayer.getPreprocessor()));
                            }
                            ArrayList<String> rel = new ArrayList<>();
                            rel.add(playerRole);
                            rel.add(playerVariable);
                            relationStrings.add(rel);
                            playerCounter++;
                        }
                    }
                } else { // single player, no columnListSeparator
                    String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter;
                    String playerRole = playerGenerator.getRoleType();
                    if (playerGenerator.getUniquePlayerId().contains("_attribute_player_")) {
                        players.add(createAttributePlayerMatchStatement(currentCleanedToken, rowCounter, playerGenerator, playerVariable, generatorMappingForPlayer.getPreprocessor()));
                    } else {
                        players.add(createEntityPlayerMatchStatement(currentCleanedToken, rowCounter, playerGenerator, playerVariable, generatorMappingForPlayer.getPreprocessor()));
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
        if (dce.getRelationPlayers() != null) {
            for (DataConfigEntry.DataConfigGeneratorMapping generatorMappingForRelationPlayer : dce.getRelationPlayers()) {
                String generatorKey = generatorMappingForRelationPlayer.getGenerator();
                ProcessorConfigEntry.ConceptGenerator playerGenerator = pce.getRelationPlayerGenerator(generatorKey);

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
                                    players.add(createRelationPlayerMatchStatementByAttribute(currentExplodedCleanedToken, rowCounter, playerGenerator, generatorMappingForRelationPlayer, playerVariable));
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
                            players.add(createRelationPlayerMatchStatementByAttribute(currentCleanedToken, rowCounter, playerGenerator, generatorMappingForRelationPlayer, playerVariable));
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

                    int maxColumnIndex = Arrays.stream(columnNameIndices).max().getAsInt();
                    if (rowTokens.length > maxColumnIndex) {
//                        String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter + "-" + insertCounter;
                        String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter;
                        String playerRole = playerGenerator.getRoleType();
                        players.addAll(createRelationPlayerMatchStatementByPlayers(rowTokens, rowCounter, columnNameIndices, playerGenerator, generatorMappingForRelationPlayer, playerVariable));
                        ArrayList<String> rel = new ArrayList<>();
                        rel.add(playerRole);
                        rel.add(playerVariable);
                        relationStrings.add(rel);
                        playerCounter++;
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
            players.add(returnRelation);
        }

        return players;
    }

    private ThingVariable<?> createEntityPlayerMatchStatement(String cleanedToken,
                                                              int rowCounter,
                                                              ProcessorConfigEntry.ConceptGenerator playerGenerator,
                                                              String playerVariable,
                                                              DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
        Thing ms = Graql
                .var(playerVariable)
                .isa(playerGenerator.getPlayerType());
        String attributeType = playerGenerator.getUniquePlayerId();
        String attributeValueType = playerGenerator.getIdValueType();
        ms = addAttributeOfColumnType(ms, attributeType, attributeValueType, cleanedToken, rowCounter, preprocessorConfig);
        return ms;
    }

    private ThingVariable<?> createAttributePlayerMatchStatement(String cleanedToken,
                                                                 int rowCounter,
                                                                 ProcessorConfigEntry.ConceptGenerator playerGenerator,
                                                                 String playerVariable,
                                                                 DataConfigEntry.DataConfigGeneratorMapping.PreprocessorConfig preprocessorConfig) {
        UnboundVariable uv = Graql.var(playerVariable);
        Attribute ms = addAttributeValueOfType(uv, playerGenerator.getIdValueType(), cleanedToken, rowCounter, preprocessorConfig);
        ms = ms.isa(playerGenerator.getPlayerType());
        return ms;
    }

    private ThingVariable<?> createRelationPlayerMatchStatementByAttribute(String cleanedToken,
                                                                           int rowCounter,
                                                                           ProcessorConfigEntry.ConceptGenerator playerGenerator,
                                                                           DataConfigEntry.DataConfigGeneratorMapping dcm,
                                                                           String playerVariable) {
        Thing ms = Graql
                .var(playerVariable)
                .isa(playerGenerator.getPlayerType());
        String attributeType = playerGenerator.getMatchByAttribute().get(dcm.getMatchByAttribute()).getAttributeType();
        String attributeValueType = playerGenerator.getMatchByAttribute().get(dcm.getMatchByAttribute()).getValueType();
        ms = addAttributeOfColumnType(ms, attributeType, attributeValueType, cleanedToken, rowCounter, dcm.getPreprocessor());
        return ms;
    }

    private ArrayList<ThingVariable<?>> createRelationPlayerMatchStatementByPlayers(String[] rowTokens, int rowCounter, int[] columnNameIndices, ProcessorConfigEntry.ConceptGenerator playerGenerator, DataConfigEntry.DataConfigGeneratorMapping dcm, String playerVariable) {
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
                String relationPlayerPlayerAttributeValueType = playerGenerator.getMatchByPlayer().get(dcm.getMatchByPlayers()[i]).getIdValueType();

                Thing relationPlayerCurrentPlayerMatchStatement = Graql.var(relationPlayerPlayerVariable).isa(relationPlayerPlayerType);
                relationPlayerCurrentPlayerMatchStatement = addAttributeOfColumnType(relationPlayerCurrentPlayerMatchStatement, relationPlayerPlayerAttributeType, relationPlayerPlayerAttributeValueType, cleanedToken, rowCounter, dcm.getPreprocessor());
                assembledMatchStatements.add(relationPlayerCurrentPlayerMatchStatement);

                // here add the matched player to the relation statement (i.e.: (role: $variable)):
                String relationPlayerPlayerRole = playerGenerator.getMatchByPlayer().get(dcm.getMatchByPlayers()[i]).getRoleType();
                ArrayList<String> rel = new ArrayList<>();
                rel.add(relationPlayerPlayerRole);
                rel.add(relationPlayerPlayerVariable);
                relationStrings.add(rel);
                i++;
            } else {
                // this ensures that only relations in which all required players are present actually enter the match statement - empty list = skip of insert
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
        for (Map.Entry<String, ProcessorConfigEntry.ConceptGenerator> generatorEntry : pce.getRelationRequiredPlayers().entrySet()) {
            if (!matchStatement.toString().contains("isa " + generatorEntry.getValue().getPlayerType())) {
                return false;
            }
            if (!insertStatement.toString().contains(generatorEntry.getValue().getRoleType())) {
                return false;
            }
        }
        // missing required attribute
        for (Map.Entry<String, ProcessorConfigEntry.ConceptGenerator> generatorEntry : pce.getRequiredAttributes().entrySet()) {
            if (!insertStatement.toString().contains("has " + generatorEntry.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }
}