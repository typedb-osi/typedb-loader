package processor;

import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Pattern;
import com.vaticle.typeql.lang.pattern.constraint.ThingConstraint;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import configuration.ConfigEntryData;
import configuration.ConfigEntryProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static processor.ProcessorUtil.*;

public class RelationInsertProcessor implements InsertProcessor {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    public final ConfigEntryData dce;
    public final ConfigEntryProcessor pce;
    private final int dataPathIndex;

    public RelationInsertProcessor(ConfigEntryData dce, ConfigEntryProcessor configEntryProcessor, int dataPathIndex) {
        super();
        this.dce = dce;
        this.pce = configEntryProcessor;
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
                ThingVariable.Relation assembledInsertStatement = relationInsert((ThingVariable.Relation) playersInsertStatement);

                if (dce.getAttributeProcessorMappings() != null) {
                    for (ConfigEntryData.ConceptProcessorMapping generatorMappingForAttribute : dce.getAttributeProcessorMappings()) {
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
        UnboundVariable relVariable = TypeQL.var("rel");
        ArrayList<ArrayList<String>> relationStrings = new ArrayList<>();
        int playerCounter = 0;

        // add Entity Players:
        for (ConfigEntryData.ConceptProcessorMapping cpm : dce.getPlayerProcessorMappings()) {
            ConfigEntryProcessor.ConceptProcessor cp = pce.getPlayerGenerator(cpm.getConceptProcessor());
            int columnNameIndex = idxOf(columnNames, cpm.getColumnName());

            if (columnNameIndex == -1) {
                appLogger.error("The column header " + cpm.getColumnName() + " specified in your dataconfig cannot be found in the file you specified.");
            }

            if (rowTokens.length > columnNameIndex && // make sure that there are enough rowTokens in the row for your column of interest
                    !cleanToken(rowTokens[columnNameIndex]).isEmpty()) { // make sure that after cleaning, there is more than an empty string
                String cleanedRecord = cleanToken(rowTokens[columnNameIndex]);
                if (cpm.getListSeparator() != null) {
                    for (String exploded : cleanedRecord.split(cpm.getListSeparator())) {
                        if (!cleanToken(exploded).isEmpty()) {
                            if (cp.getUniquePlayerId().contains("_attribute_player_")) {
                                matches.add(createAttributePlayerMatchStatement(
                                        cleanToken(exploded),
                                        rowCounter,
                                        cp.getPlayerType() + "-" + playerCounter,
                                        cp.getPlayerType(),
                                        cp.getUniquePlayerId(),
                                        cp.getIdValueType(),
                                        cpm.getPreprocessor()));
                            } else {
                                matches.add(generateThingPlayerMatchByAttributeStatement(
                                        cleanToken(exploded),
                                        rowCounter,
                                        cp.getPlayerType() + "-" + playerCounter,
                                        cp.getPlayerType(),
                                        cp.getUniquePlayerId(),
                                        cp.getIdValueType(),
                                        cpm.getPreprocessor()));
                            }
                            relationStrings.add(new ArrayList<>(Arrays.asList(cp.getRoleType(), cp.getPlayerType() + "-" + playerCounter)));
                            playerCounter++;
                        }
                    }
                } else { // single player, no columnListSeparator
                    if (cp.getUniquePlayerId().contains("_attribute_player_")) {
                        matches.add(createAttributePlayerMatchStatement(
                                cleanedRecord,
                                rowCounter,
                                cp.getPlayerType() + "-" + playerCounter,
                                cp.getPlayerType(),
                                cp.getUniquePlayerId(),
                                cp.getIdValueType(),
                                cpm.getPreprocessor()));
                    } else {
                        matches.add(generateThingPlayerMatchByAttributeStatement(
                                cleanedRecord,
                                rowCounter,
                                cp.getPlayerType() + "-" + playerCounter,
                                cp.getPlayerType(),
                                cp.getUniquePlayerId(),
                                cp.getIdValueType(),
                                cpm.getPreprocessor()));
                    }
                    relationStrings.add(new ArrayList<>(Arrays.asList(cp.getRoleType(), cp.getPlayerType() + "-" + playerCounter)));
                    playerCounter++;
                }
            }
        }
        // add Relation Players
        if (dce.getRelationPlayerProcessorMappings() != null) {
            for (ConfigEntryData.ConceptProcessorMapping cpm : dce.getRelationPlayerProcessorMappings()) {
                ConfigEntryProcessor.ConceptProcessor cp = pce.getRelationPlayerGenerator(cpm.getConceptProcessor());

                // if matching RelationPlayer by Attribute:
                if (cpm.getMatchByAttribute() != null) {
                    int columnNameIndex = idxOf(columnNames, cpm.getColumnName());
                    if (columnNameIndex == -1) {
                        appLogger.error("The column header " + cpm.getColumnName() + " specified in your dataconfig cannot be found in the file you specified.");
                    }

                    if (rowTokens.length > columnNameIndex &&
                            !cleanToken(rowTokens[columnNameIndex]).isEmpty()) {
                        String cleanedToken = cleanToken(rowTokens[columnNameIndex]);
                        if (cpm.getListSeparator() != null) {
                            for (String exploded : cleanedToken.split(cpm.getListSeparator())) {
                                if (!cleanToken(exploded).isEmpty()) {
                                    matches.add(generateThingPlayerMatchByAttributeStatement(
                                            cleanToken(exploded),
                                            rowCounter,
                                            cp.getPlayerType() + "-" + playerCounter,
                                            cp.getPlayerType(),
                                            cp.getMatchByAttribute().get(cpm.getMatchByAttribute()).getAttributeType(),
                                            cp.getMatchByAttribute().get(cpm.getMatchByAttribute()).getValueType(),
                                            cpm.getPreprocessor()));
                                    relationStrings.add(new ArrayList<>(Arrays.asList(cp.getRoleType(), cp.getPlayerType() + "-" + playerCounter)));
                                    playerCounter++;
                                }
                            }
                        } else { // single player, no listSeparator
                            matches.add(generateThingPlayerMatchByAttributeStatement(
                                    cleanedToken,
                                    rowCounter,
                                    cp.getPlayerType() + "-" + playerCounter,
                                    cp.getPlayerType(),
                                    cp.getMatchByAttribute().get(cpm.getMatchByAttribute()).getAttributeType(),
                                    cp.getMatchByAttribute().get(cpm.getMatchByAttribute()).getValueType(),
                                    cpm.getPreprocessor()));
                            relationStrings.add(new ArrayList<>((Arrays.asList(cp.getRoleType(), cp.getPlayerType() + "-" + playerCounter))));
                            playerCounter++;
                        }
                    }
                    // if matching the relationStrings player by players in that relationStrings:
                } else if (cpm.getMatchByPlayers().length > 0) {
                    int[] columnNameIndices = indicesOf(columnNames, cpm.getColumnNames());

                    for (int i : columnNameIndices) {
                        if (i == -1) {
                            appLogger.error("The column header " + cpm.getColumnName() + " specified in your dataconfig cannot be found in the file you specified.");
                        }
                    }

                    if (Arrays.stream(columnNameIndices).max().isPresent()) {
                        int maxColumnIndex = Arrays.stream(columnNameIndices).max().getAsInt();
                        if (rowTokens.length > maxColumnIndex) {
                            String playerVariable = cp.getPlayerType() + "-" + playerCounter;
                            String playerRole = cp.getRoleType();
                            matches.addAll(createRelationPlayerMatchStatementByPlayers(rowTokens, rowCounter, columnNameIndices, cp, cpm, playerVariable));
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
                    appLogger.error("Your config entry for column header: " + cpm.getColumnName() + "needs to specify matching either by player/s or by an attribute");
                }
            }
        }

        if (relationStrings.size() >= 1) {
            ThingVariable.Relation returnRelation = relVariable.rel(relationStrings.get(0).get(0), relationStrings.get(0).get(1));
            for (ArrayList<String> rel : relationStrings.subList(1, relationStrings.size())) {
                returnRelation = returnRelation.rel(rel.get(0), rel.get(1));
            }
            matches.add(returnRelation);
        }

        return matches;
    }

    private ThingVariable<?> generateThingPlayerMatchByAttributeStatement(String cleanedToken,
                                                                          int rowCounter,
                                                                          String playerVariable,
                                                                          String playerType,
                                                                          String attributeType,
                                                                          AttributeValueType attributeValueType,
                                                                          ConfigEntryData.ConceptProcessorMapping.PreprocessorConfig preprocessorConfig) {
        return TypeQL
                .var(playerVariable)
                .isa(playerType)
                .constrain(valueToHasConstraint(attributeType, generateValueConstraint(attributeType, attributeValueType, cleanedToken, rowCounter, preprocessorConfig)));
    }

    private ThingVariable<?> createAttributePlayerMatchStatement(String cleanedToken,
                                                                 int rowCounter,
                                                                 String playerVariable,
                                                                 String playerType,
                                                                 String attributeType,
                                                                 AttributeValueType attributeValueType,
                                                                 ConfigEntryData.ConceptProcessorMapping.PreprocessorConfig preprocessorConfig) {
        return TypeQL
                .var(playerVariable)
                .constrain(generateValueConstraint(attributeType, attributeValueType, cleanedToken, rowCounter, preprocessorConfig))
                .isa(playerType);
    }

    private ArrayList<ThingVariable<?>> createRelationPlayerMatchStatementByPlayers(String[] rowTokens, int rowCounter, int[] columnNameIndices, ConfigEntryProcessor.ConceptProcessor playerGenerator, ConfigEntryData.ConceptProcessorMapping dcm, String playerVariable) {
        ArrayList<ThingVariable<?>> assembledMatchStatements = new ArrayList<>();
        UnboundVariable relVariable = TypeQL.var(playerVariable);
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

                ThingVariable.Thing relationPlayerCurrentPlayerMatchStatement = TypeQL.var(relationPlayerPlayerVariable).isa(relationPlayerPlayerType);
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
            ThingVariable.Relation returnRelation = relVariable.rel(relationStrings.get(0).get(0), relationStrings.get(0).get(1));
            for (ArrayList<String> rel : relationStrings.subList(1, relationStrings.size())) {
                returnRelation = returnRelation.rel(rel.get(0), rel.get(1));
            }
            assembledMatchStatements.add(returnRelation.isa(playerGenerator.getPlayerType()));
        }

        return assembledMatchStatements;
    }


    private ThingVariable.Relation relationInsert(ThingVariable.Relation si) {
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
        for (Map.Entry<String, ConfigEntryProcessor.ConceptProcessor> generatorEntry : pce.getRelationRequiredPlayers().entrySet()) {
            if (!matchStatement.toString().contains("isa " + generatorEntry.getValue().getPlayerType())) {
                return false;
            }
            if (!insertStatement.toString().contains(generatorEntry.getValue().getRoleType())) {
                return false;
            }
        }
        // missing required attribute
        for (Map.Entry<String, ConfigEntryProcessor.ConceptProcessor> generatorEntry : pce.getRequiredAttributes().entrySet()) {
            if (!insertStatement.toString().contains("has " + generatorEntry.getValue().getAttributeType())) {
                return false;
            }
        }
        return true;
    }
}