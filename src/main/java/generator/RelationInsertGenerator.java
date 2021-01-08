package generator;

import static generator.GeneratorUtil.idxOf;
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

    private final DataConfigEntry dce;
    private final ProcessorConfigEntry gce;
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

        String[] tokens = row.split(dce.getSeparator());
        String[] headerTokens = header.split(dce.getSeparator());
        appLogger.debug("processing tokenized row: " + Arrays.toString(tokens));
        GeneratorUtil.malformedRow(row, tokens, headerTokens.length);

        ArrayList<Statement> miStatements = new ArrayList<>(createPlayerMatchAndInsert(tokens, headerTokens, insertCounter));
        ArrayList<Statement> matchStatements = new ArrayList<>(miStatements.subList(0, miStatements.size() - 1));
        ArrayList<Statement> insertStatements = new ArrayList<>();

        if (!matchStatements.isEmpty()) {
            StatementInstance playerInsert = (StatementInstance) miStatements.subList(miStatements.size() - 1, miStatements.size()).get(0);
            StatementInstance insert = relationInsert(playerInsert);
            if (dce.getAttributes() != null) {
                for (DataConfigEntry.GeneratorSpecification attDataConfigEntry : dce.getAttributes()) {
                    insert = addAttribute(tokens, insert, headerTokens, attDataConfigEntry, gce.getAttributeGenerator(attDataConfigEntry.getGenerator()));
                }
            }
            insertStatements.add(insert);

            ArrayList<ArrayList<Statement>> queries = new ArrayList<>();
            queries.add(matchStatements);
            queries.add(insertStatements);

            if (isValid(queries)) {
                appLogger.debug("valid query: <" + assembleQuery(queries) + ">");
                return queries;
            } else {
                dataLogger.warn("in datapath <" + dce.getDataPath() + ">: skipped row b/c does not have a proper <isa> statement or is missing required players or attributes. Faulty tokenized row: " + Arrays.toString(tokens));
                return null;
            }
        } else {
            dataLogger.warn("in datapath <" + dce.getDataPath() + ">: skipped row b/c has 0 players. Faulty tokenized row: " + Arrays.toString(tokens));
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

    private Collection<? extends Statement> createPlayerMatchAndInsert(String[] tokens, String[] headerTokens, int insertCounter) {
        ArrayList<Statement> players = new ArrayList<>();
        Statement playersInsertStatement = Graql.var("rel-" + insertCounter);
        int playerCounter = 0;
        for (DataConfigEntry.GeneratorSpecification playerDataConfigEntry : dce.getPlayers()) {
            ProcessorConfigEntry.ConceptGenerator playerGenerator = gce.getPlayerGenerator(playerDataConfigEntry.getGenerator());
            int playerDataIndex = idxOf(headerTokens, playerDataConfigEntry);

            if(playerDataIndex == -1) {
                appLogger.error("The column header in your dataconfig mapping to the uniquePlayerId [" + playerGenerator.getUniquePlayerId() + "] cannot be found in the file you specified.");
            }

            if (tokens.length > playerDataIndex && // make sure that there are enough tokens in the row for your column of interest
                    !cleanToken(tokens[playerDataIndex]).isEmpty()) { // make sure that after cleaning, there is more than an empty string
                String listSeparator = playerDataConfigEntry.getListSeparator();
                if(listSeparator != null) {
                    for (String exploded: tokens[playerDataIndex].split(listSeparator)) {
                        if(!cleanToken(exploded).isEmpty()) {
                            String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter + "-" + insertCounter;
                            players.add(createPlayerMatchStatement(exploded, playerGenerator, playerVariable));
                            playersInsertStatement = playersInsertStatement.rel(playerGenerator.getRoleType(), playerVariable);
                            playerCounter++;
                        }
                    }
                } else { // single player, no listSeparator
                    String playerVariable = playerGenerator.getPlayerType() + "-" + playerCounter + "-" + insertCounter;
                    players.add(createPlayerMatchStatement(cleanToken(tokens[playerDataIndex]), playerGenerator, playerVariable));
                    playersInsertStatement = playersInsertStatement.rel(playerGenerator.getRoleType(), playerVariable);
                    playerCounter++;
                }
            }
        }
        players.add(playersInsertStatement);
        return players;
    }

    private StatementInstance createPlayerMatchStatement(String token, ProcessorConfigEntry.ConceptGenerator playerGenerator, String playerVariable) {
        String cleanedValue = cleanToken(token);
        StatementInstance ms = Graql
                .var(playerVariable)
                .isa(playerGenerator.getPlayerType());
        ms = addAttributeOfColumnType(ms, playerGenerator.getUniquePlayerId(), playerGenerator.getIdValueType(), cleanedValue);
                //.has(playerGenerator.getUniquePlayerId(), cleanedValue);
        return ms;
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