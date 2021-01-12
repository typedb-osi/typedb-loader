package generator;

import static generator.GeneratorUtil.idxOf;
import static generator.GeneratorUtil.indicesOf;
import static generator.GeneratorUtil.cleanToken;
import static generator.GeneratorUtil.addAttribute;
import static generator.GeneratorUtil.addAttributeOfColumnType;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import graql.lang.Graql;
import graql.lang.statement.Statement;
import graql.lang.statement.StatementInstance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class RelationInsertGenerator extends InsertGenerator {

    public final DataConfigEntry dce;
    public final ProcessorConfigEntry gce;
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");

    public RelationInsertGenerator(DataConfigEntry dce, ProcessorConfigEntry processorConfigEntry) {
        super();
        this.dce = dce;
        this.gce = processorConfigEntry;
        appLogger.debug("Creating RelationInsertGenerator for " + gce.getProcessor() + " of type " + gce.getProcessorType());
    }

    public ArrayList<ArrayList<ArrayList<Statement>>> graknRelationInsert(ArrayList<String> rows, String header) throws Exception {
        ArrayList<ArrayList<ArrayList<Statement>>> matchInsertStatements = new ArrayList<>();

        ArrayList<ArrayList<Statement>> matchStatements = new ArrayList<>();
        ArrayList<ArrayList<Statement>> insertStatements = new ArrayList<>();

        int insertCounter = 0;

        for (String row : rows) {
            ArrayList<ArrayList<Statement>> tmp = graknRelationshipQueryFromRow(row, header, insertCounter);
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

    public ArrayList<ArrayList<Statement>> graknRelationshipQueryFromRow(String row, String header, int insertCounter) throws Exception {

        String[] rowTokens = row.split(dce.getSeparator());
        String[] columnNames = header.split(dce.getSeparator());
        appLogger.debug("processing tokenized row: " + Arrays.toString(rowTokens));
        GeneratorUtil.malformedRow(row, rowTokens, columnNames.length);

        ArrayList<Statement> miStatements = new ArrayList<>(createPlayerMatchAndInsert(rowTokens, columnNames, insertCounter));
        ArrayList<Statement> matchStatements = new ArrayList<>(miStatements.subList(0, miStatements.size() - 1));
        ArrayList<Statement> insertStatements = new ArrayList<>();

        if (!matchStatements.isEmpty()) {
            StatementInstance playersInsertStatement = (StatementInstance) miStatements.subList(miStatements.size() - 1, miStatements.size()).get(0);
            StatementInstance assembledInsertStatement = relationInsert(playersInsertStatement);

            if (dce.getAttributes() != null) {
                for (DataConfigEntry.dataConfigGeneratorMapping generatorMappingForAttribute : dce.getAttributes()) {
                    String attributeGeneratorKey = generatorMappingForAttribute.getGenerator();
                    ProcessorConfigEntry.ConceptGenerator attributeGenerator = gce.getAttributeGenerator(attributeGeneratorKey);
                    assembledInsertStatement = addAttribute(rowTokens, assembledInsertStatement, columnNames, generatorMappingForAttribute, attributeGenerator);
                }
            }
            insertStatements.add(assembledInsertStatement);

            ArrayList<ArrayList<Statement>> assembledStatements = new ArrayList<>();
            assembledStatements.add(matchStatements);
            assembledStatements.add(insertStatements);

            if (isValid(assembledStatements)) {
                appLogger.debug("valid query: <" + assembleQuery(assembledStatements) + ">");
                return assembledStatements;
            } else {
                dataLogger.warn("in datapath <" + dce.getDataPath() + ">: skipped row b/c does not have a proper <isa> statement or is missing required players or attributes. Faulty tokenized row: " + Arrays.toString(rowTokens));
                return null;
            }
        } else {
            dataLogger.warn("in datapath <" + dce.getDataPath() + ">: skipped row b/c has 0 players. Faulty tokenized row: " + Arrays.toString(rowTokens));
            return null;
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

    private Collection<? extends Statement> createPlayerMatchAndInsert(String[] rowTokens, String[] columnNames, int insertCounter) {
        ArrayList<Statement> players = new ArrayList<>();
        Statement playersInsertStatement = Graql.var("rel-" + insertCounter);
        int playerCounter = 0;

        // add Entity Players:
        for (DataConfigEntry.dataConfigGeneratorMapping generatorMappingForPlayer : dce.getPlayers()) {
            String generatorKey = generatorMappingForPlayer.getGenerator();
            ProcessorConfigEntry.ConceptGenerator playerGenerator = gce.getPlayerGenerator(generatorKey);
            int columnNameIndex = idxOf(columnNames, generatorMappingForPlayer);

            if(columnNameIndex == -1) {
                appLogger.error("The column header " + generatorMappingForPlayer.getColumnName() + " specified in your dataconfig cannot be found in the file you specified.");
            }

            if (rowTokens.length > columnNameIndex && // make sure that there are enough rowTokens in the row for your column of interest
                    !cleanToken(rowTokens[columnNameIndex]).isEmpty()) { // make sure that after cleaning, there is more than an empty string
                String currentCleanedToken = cleanToken(rowTokens[columnNameIndex]);
                String columnListSeparator = generatorMappingForPlayer.getListSeparator();
                if(columnListSeparator != null) {
                    for (String exploded: currentCleanedToken.split(columnListSeparator)) {
                        if(!cleanToken(exploded).isEmpty()) {
                            String currentExplodedCleanedToken = cleanToken(exploded);
                            String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter + "-" + insertCounter;
                            String playerRole = playerGenerator.getRoleType();
                            players.add(createPlayerMatchStatement(currentExplodedCleanedToken, playerGenerator, playerVariable));
                            playersInsertStatement = playersInsertStatement.rel(playerRole, playerVariable);
                            playerCounter++;
                        }
                    }
                } else { // single player, no columnListSeparator
                    String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter + "-" + insertCounter;
                    String playerRole = playerGenerator.getRoleType();
                    players.add(createPlayerMatchStatement(currentCleanedToken, playerGenerator, playerVariable));
                    playersInsertStatement = playersInsertStatement.rel(playerRole, playerVariable);
                    playerCounter++;
                }
            }
        }
        // add Relation Players
        if (dce.getRelationPlayers() != null) {
            for (DataConfigEntry.dataConfigGeneratorMapping generatorMappingForRelationPlayer : dce.getRelationPlayers()) {
                String generatorKey = generatorMappingForRelationPlayer.getGenerator();
                ProcessorConfigEntry.ConceptGenerator playerGenerator = gce.getRelationPlayerGenerator(generatorKey);

                // if matching RelationPlayer by Attribute:
                if (generatorMappingForRelationPlayer.getMatchByAttribute() != null) {
                    int columnNameIndex = idxOf(columnNames, generatorMappingForRelationPlayer);

                    if(columnNameIndex == -1) {
                        appLogger.error("The column header " + generatorMappingForRelationPlayer.getColumnName() + " specified in your dataconfig cannot be found in the file you specified.");
                    }

                    if (rowTokens.length > columnNameIndex &&
                            !cleanToken(rowTokens[columnNameIndex]).isEmpty()) {
                        String currentCleanedToken = cleanToken(rowTokens[columnNameIndex]);
                        String columnListSeparator = generatorMappingForRelationPlayer.getListSeparator();
                        if(columnListSeparator != null) {
                            for (String exploded: currentCleanedToken.split(columnListSeparator)) {
                                if(!cleanToken(exploded).isEmpty()) {
                                    String currentExplodedCleanedToken = cleanToken(exploded);
                                    String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter + "-" + insertCounter;
                                    String playerRole = playerGenerator.getRoleType();
                                    players.add(createRelationPlayerMatchStatementByAttribute(currentExplodedCleanedToken, playerGenerator, generatorMappingForRelationPlayer, playerVariable));
                                    playersInsertStatement = playersInsertStatement.rel(playerRole, playerVariable);
                                    playerCounter++;
                                }
                            }
                        } else { // single player, no listSeparator
                            String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter + "-" + insertCounter;
                            String playerRole = playerGenerator.getRoleType();
                            players.add(createRelationPlayerMatchStatementByAttribute(currentCleanedToken, playerGenerator, generatorMappingForRelationPlayer, playerVariable));
                            playersInsertStatement = playersInsertStatement.rel(playerRole, playerVariable);
                            playerCounter++;
                        }
                    }
                // if matching the relation player by players in that relation:
                } else if (generatorMappingForRelationPlayer.getMatchByPlayers().length > 0) {
                    int[] columnNameIndices = indicesOf(columnNames, generatorMappingForRelationPlayer);

                    for (int i : columnNameIndices) {
                        if(i == -1) {
                            appLogger.error("The column header " + generatorMappingForRelationPlayer.getColumnName() + " specified in your dataconfig cannot be found in the file you specified.");
                        }
                    }

                    int maxColumnIndex = Arrays.stream(columnNameIndices).max().getAsInt();
                    if (rowTokens.length > maxColumnIndex) {
                        String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter + "-" + insertCounter;
                        String playerRole = playerGenerator.getRoleType();
                        players.addAll(createRelationPlayerMatchStatementByPlayers(rowTokens, columnNameIndices, playerGenerator, generatorMappingForRelationPlayer, playerVariable, insertCounter));
                        playersInsertStatement = playersInsertStatement.rel(playerRole, playerVariable);
                        playerCounter++;
                    }
                } else {
                    appLogger.error("Your config entry for column header: " + generatorMappingForRelationPlayer.getColumnName() + "needs to specify matching either by player/s or by an attribute");
                }
            }
        }

        players.add(playersInsertStatement);
        return players;
    }

    private StatementInstance createPlayerMatchStatement(String cleanedToken, ProcessorConfigEntry.ConceptGenerator playerGenerator, String playerVariable) {
        StatementInstance ms = Graql
                .var(playerVariable)
                .isa(playerGenerator.getPlayerType());
        String attributeType = playerGenerator.getUniquePlayerId();
        String attributeValueType = playerGenerator.getIdValueType();
        ms = addAttributeOfColumnType(ms, attributeType, attributeValueType, cleanedToken);
        return ms;
    }

    private StatementInstance createRelationPlayerMatchStatementByAttribute(String cleanedToken, ProcessorConfigEntry.ConceptGenerator playerGenerator, DataConfigEntry.dataConfigGeneratorMapping dataConfigMapping, String playerVariable) {
        StatementInstance ms = Graql
                .var(playerVariable)
                .isa(playerGenerator.getPlayerType());
        String attributeType = playerGenerator.getMatchByAttribute().get(dataConfigMapping.getMatchByAttribute()).getAttributeType();
        String attributeValueType = playerGenerator.getMatchByAttribute().get(dataConfigMapping.getMatchByAttribute()).getValueType();
        ms = addAttributeOfColumnType(ms, attributeType, attributeValueType, cleanedToken);
        return ms;
    }

    private ArrayList<Statement> createRelationPlayerMatchStatementByPlayers(String[] rowTokens, int[] columnNameIndices, ProcessorConfigEntry.ConceptGenerator playerGenerator, DataConfigEntry.dataConfigGeneratorMapping dce, String playerVariable, int insertCounter) {
        ArrayList<Statement> assembledMatchStatements = new ArrayList<>();

        Statement relationPlayerMatchStatement = Graql.var(playerVariable);

        //match the n entites with their attributes
        int i = 0;
        for (int columnNameIndex : columnNameIndices) {
            if (!cleanToken(rowTokens[columnNameIndex]).isEmpty()) {
                String cleanedToken = cleanToken(rowTokens[columnNameIndex]);
                String relationPlayerPlayerVariable = "relplayer-player-" + insertCounter + "-" + i;
                String relationPlayerPlayerType = playerGenerator.getMatchByPlayer().get(dce.getMatchByPlayers()[i]).getPlayerType();
                String relationPlayerPlayerAttributeType = playerGenerator.getMatchByPlayer().get(dce.getMatchByPlayers()[i]).getUniquePlayerId();
                String relationPlayerPlayerAttributeValueType = playerGenerator.getMatchByPlayer().get(dce.getMatchByPlayers()[i]).getIdValueType();

                StatementInstance relationPlayerCurrentPlayerMatchStatement = Graql.var(relationPlayerPlayerVariable).isa(relationPlayerPlayerType);
                relationPlayerCurrentPlayerMatchStatement = addAttributeOfColumnType(relationPlayerCurrentPlayerMatchStatement, relationPlayerPlayerAttributeType, relationPlayerPlayerAttributeValueType, cleanedToken);
                assembledMatchStatements.add(relationPlayerCurrentPlayerMatchStatement);

                // here add the matched player to the relation statement (i.e.: (role: $variable)):
                String relationPlayerPlayerRole = playerGenerator.getMatchByPlayer().get(dce.getMatchByPlayers()[i]).getRoleType();
                relationPlayerMatchStatement = relationPlayerMatchStatement.rel(relationPlayerPlayerRole, relationPlayerPlayerVariable);
                i++;
            } else {
                // this ensures that only relations in which all required players are present actually enter the match statement - empty list = skip of insert
                boolean requiredButNotPresent = playerGenerator.getMatchByPlayer().get(dce.getMatchByPlayers()[i]).isRequired();
                if (requiredButNotPresent) {
                    return new ArrayList<>();
                }
            }
        }

        if (assembledMatchStatements.size() > 0) {
            // complete the relation match statement & add to assembly:
            relationPlayerMatchStatement = relationPlayerMatchStatement.isa(playerGenerator.getPlayerType());
            assembledMatchStatements.add(relationPlayerMatchStatement);
        }

        return assembledMatchStatements;
    }


    private StatementInstance relationInsert(StatementInstance si) {
        if (si != null) {
            si = si.isa(gce.getSchemaType());
            return si;
        } else {
            return null;
        }
    }

    private boolean isValid(ArrayList<ArrayList<Statement>> si) {
        ArrayList<Statement> matchStatements = si.get(0);
        ArrayList<Statement> insertStatements = si.get(1);
        StringBuilder matchStatement = new StringBuilder();
        for (Statement st:matchStatements) {
            matchStatement.append(st.toString());
        }
        String insertStatement = insertStatements.get(0).toString();
        // missing required players
        for (Map.Entry<String, ProcessorConfigEntry.ConceptGenerator> generatorEntry: gce.getRelationRequiredPlayers().entrySet()) {
            if (!matchStatement.toString().contains("isa " + generatorEntry.getValue().getPlayerType())) {
                return false;
            }
            if (!insertStatement.contains(generatorEntry.getValue().getRoleType())) {
                return false;
            }
        }
        // missing required attribute
        for (Map.Entry<String, ProcessorConfigEntry.ConceptGenerator> generatorEntry: gce.getRequiredAttributes().entrySet()) {
            if (!insertStatement.contains("has " + generatorEntry.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }
}