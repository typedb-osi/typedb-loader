package a_rewrite.generator;

import a_rewrite.config.Configuration;
import a_rewrite.config.TypeHandler;
import a_rewrite.io.ErrorFileLogger;
import a_rewrite.util.GeneratorUtil;
import a_rewrite.util.Util;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typedb.client.common.exception.TypeDBClientException;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.constraint.ThingConstraint;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

public class RelationGenerator implements Generator {
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private final String filePath;
    private final String[] header;
    private final Configuration.Relation relationConfiguration;
    private final char fileSeparator;

    public RelationGenerator(String filePath, Configuration.Relation relationConfiguration, char fileSeparator) throws IOException {
        this.filePath = filePath;
        this.header = Util.getFileHeader(filePath, fileSeparator);
        this.relationConfiguration = relationConfiguration;
        this.fileSeparator = fileSeparator;
    }

    public void write(TypeDBTransaction tx,
                      String[] row) throws Exception {
        String fileName = FilenameUtils.getName(filePath);
        String fileNoExtension = FilenameUtils.removeExtension(fileName);
        String originalRow = String.join(Character.toString(fileSeparator), row);

        if (row.length > header.length) {
            ErrorFileLogger.getLogger().logMalformed(fileName, originalRow);
            dataLogger.warn("Malformed Row detected in <" + filePath + "> - written to <" + fileNoExtension + "_malformed.log" + ">");
        }

        TypeQLInsert statement = generateMatchInsertStatement(row);

        if (relationInsertStatementValid(statement)) {
            try {
                tx.query().insert(statement);
            } catch (TypeDBClientException typeDBClientException) {
                ErrorFileLogger.getLogger().logUnavailable(fileName, originalRow);
                appLogger.warn("TypeDB Unavailable - Row in <" + filePath + "> not inserted - written to <" + fileNoExtension + "_unavailable.log" + ">");
            }
        } else {
            ErrorFileLogger.getLogger().logInvalid(fileName, originalRow);
            dataLogger.warn("Invalid Row detected in <" + filePath + "> - written to <" + fileNoExtension + "_invalid.log" + "> - invalid Statement: " + statement.toString());
        }
    }

    public TypeQLInsert generateMatchInsertStatement(String[] row) {
        if (row.length > 0) {
            ArrayList<ThingVariable<?>> playerMatchStatements = new ArrayList<>();
            //TODO: replace this String solution with language builder
            ArrayList<String> playerVars = new ArrayList<>();
            ArrayList<String> roleTypes = new ArrayList<>();

            int playerIdx = 0;
            for (Configuration.Player player : relationConfiguration.getPlayers()) {
                String playerVar = "player-" + playerIdx;
                TypeHandler playerTypeHandler = getPlayerType(player);
                assert playerTypeHandler != null;

                // ATTRIBUTE PLAYER
                if (playerTypeHandler.equals(TypeHandler.ATTRIBUTE)) {
                    ThingVariable.Attribute playerMatchStatement = getAttributePlayerMatchStatement(row, player, playerVar);
                    if (playerMatchStatement != null) {
                        playerMatchStatements.add(playerMatchStatement);
                        playerVars.add(playerVar);
                        roleTypes.add(player.getRoleType());
                        playerIdx += 1;
                    }
                }

                // ENTITY PLAYER BY ATTRIBUTE(s)
                if (playerTypeHandler.equals(TypeHandler.ENTITY)) {
                    ThingVariable.Thing playerMatchStatement = getThingPlayerMatchStatementByAttribute(row, player, playerVar);
                    if (playerMatchStatement.toString().contains(", has")) {
                        playerMatchStatements.add(playerMatchStatement);
                        playerVars.add(playerVar);
                        roleTypes.add(player.getRoleType());
                        playerIdx += 1;
                    }
                }

                // RELATION PLAYER BY ATTRIBUTE(s)
                if (playerTypeHandler.equals(TypeHandler.RELATION)) {
                    boolean isByAttribute = nestedRelationByAttribute(player);
                    if (isByAttribute) {
                        ThingVariable.Thing playerMatchStatement = getThingPlayerMatchStatementByAttribute(row, player, playerVar);
                        if (playerMatchStatement.toString().contains(", has")) {
                            playerMatchStatements.add(playerMatchStatement);
                            playerVars.add(playerVar);
                            roleTypes.add(player.getRoleType());
                            playerIdx += 1;
                        }
                    } else {
                        //TODO: here now identify not by attribute but by players (either entity OR attribute)
                        ArrayList<ThingVariable<?>> playerMatchStatement = getRelationPlayerMatchStatementByPlayer(row, player, playerVar);
                        if (playerMatchStatement != null) { //TODO: check if this makes sense, see above for examples
                            playerMatchStatements.addAll(playerMatchStatement);
                            playerVars.add(playerVar);
                            roleTypes.add(player.getRoleType());
                            playerIdx += 1;
                        }
                    }

                }
            }

            ThingVariable.Relation insertStatement = null;
            for (int i = 0; i < roleTypes.size(); i++) {
                if (insertStatement == null) {
                    insertStatement = TypeQL.var("rel").rel(roleTypes.get(i), playerVars.get(i));
                } else {
                    insertStatement = insertStatement.rel(roleTypes.get(i), playerVars.get(i));
                }
            }
            if (insertStatement != null) {
                insertStatement = insertStatement.isa(relationConfiguration.getConceptType());
                if (relationConfiguration.getAttributes() != null) {
                    for (Configuration.HasAttribute hasAttribute : relationConfiguration.getAttributes()) {
                        ArrayList<ThingConstraint.Value<?>> tmp = GeneratorUtil.generateValueConstraints(
                                row[GeneratorUtil.getColumnIndexByName(header, hasAttribute.getColumn())],
                                hasAttribute.getConceptType(),
                                hasAttribute.getConceptValueType(),
                                hasAttribute.getListSeparator(),
                                hasAttribute.getPreprocessorConfig(),
                                row,
                                filePath,
                                fileSeparator);
                        for (ThingConstraint.Value<?> constraintValue : tmp) {
                            insertStatement.constrain(GeneratorUtil.valueToHasConstraint(hasAttribute.getConceptType(), constraintValue));
                        }
                    }
                }

                return TypeQL.match(playerMatchStatements).insert(insertStatement);
            } else {
                return TypeQL.insert(TypeQL.var("null").isa("null").has("null", "null"));
            }
        } else {
            return TypeQL.insert(TypeQL.var("null").isa("null").has("null", "null"));
        }
    }

    private boolean nestedRelationByAttribute(Configuration.Player player) {
        for (Configuration.ThingGetter thingGetter : player.getRoleGetter().getThingGetters()) {
            if (thingGetter.getHandler() != TypeHandler.OWNERSHIP) return false;
        }
        return player.getRoleGetter().getOwnershipThingGetters().length > 0;
    }

    private ThingVariable.Thing getThingPlayerMatchStatementByAttribute(String[] row, Configuration.Player player, String playerVar) {
        ThingVariable.Thing playerMatchStatement = TypeQL.var(playerVar)
                .isa(player.getRoleGetter().getConceptType());
        for (Configuration.ThingGetter ownershipThingGetter : player.getRoleGetter().getOwnershipThingGetters()) {
            ArrayList<ThingConstraint.Value<?>> tmp = GeneratorUtil.generateValueConstraints(
                    row[GeneratorUtil.getColumnIndexByName(header, ownershipThingGetter.getColumn())],
                    ownershipThingGetter.getConceptType(),
                    ownershipThingGetter.getConceptValueType(),
                    ownershipThingGetter.getListSeparator(),
                    ownershipThingGetter.getPreprocessorConfig(),
                    row,
                    filePath,
                    fileSeparator);
            for (ThingConstraint.Value<?> constraintValue : tmp) {
                playerMatchStatement.constrain(GeneratorUtil.valueToHasConstraint(ownershipThingGetter.getConceptType(), constraintValue));
            }
        }
        return playerMatchStatement;
    }

    private ThingVariable.Attribute getAttributePlayerMatchStatement(String[] row, Configuration.Player player, String playerVar) {
        Configuration.RoleGetter attributeRoleGetter = player.getRoleGetter();
        ArrayList<ThingConstraint.Value<?>> constraints = GeneratorUtil.generateValueConstraints(
                row[GeneratorUtil.getColumnIndexByName(header, attributeRoleGetter.getColumn())],
                attributeRoleGetter.getConceptType(),
                attributeRoleGetter.getConceptValueType(),
                null,
                attributeRoleGetter.getPreprocessorConfig(),
                row,
                filePath,
                fileSeparator);
        if (constraints.size() > 0) {
            return TypeQL.var(playerVar)
                    .constrain(constraints.get(0))
                    .isa(attributeRoleGetter.getConceptType());
        } else {
            return null;
        }
    }

    private ArrayList<ThingVariable<?>> getRelationPlayerMatchStatementByPlayer(String[] row, Configuration.Player player, String playerVar) {

        // create a playerSubVar
        ArrayList<ThingVariable.Thing> subPlayerMatchStatements = new ArrayList<>();
        ArrayList<String> subPlayerVars = new ArrayList<>();
        int subPlayerVarCounter = 0;
        String subPlayerVar;
        for (Configuration.ThingGetter thingGetter : player.getRoleGetter().getThingGetters()) {
            subPlayerVar = playerVar + "-" + subPlayerVarCounter;
            ThingVariable.Thing subPlayerMatchStatement = TypeQL.var(subPlayerVar)
                    .isa(thingGetter.getConceptType());
            for (Configuration.ThingGetter thingThingGetter : thingGetter.getThingGetters()) {
                ArrayList<ThingConstraint.Value<?>> tmp = GeneratorUtil.generateValueConstraints(
                        row[GeneratorUtil.getColumnIndexByName(header, thingThingGetter.getColumn())],
                        thingThingGetter.getConceptType(),
                        thingThingGetter.getConceptValueType(),
                        thingThingGetter.getListSeparator(),
                        thingThingGetter.getPreprocessorConfig(),
                        row,
                        filePath,
                        fileSeparator);
                for (ThingConstraint.Value<?> constraintValue : tmp) {
                    subPlayerMatchStatement.constrain(GeneratorUtil.valueToHasConstraint(thingThingGetter.getConceptType(), constraintValue));
                }
            }
            if (subEntityValid(subPlayerMatchStatement, thingGetter)) {
                subPlayerMatchStatements.add(subPlayerMatchStatement);
                subPlayerVars.add(subPlayerVar);
                subPlayerVarCounter++;
            }
        }

        ThingVariable.Relation relationMatchStatement = null;
        for (int i = 0; i < subPlayerMatchStatements.size(); i++) {
            if (relationMatchStatement == null) {
                relationMatchStatement = TypeQL.var(playerVar).rel(player.getRoleGetter().getThingGetters()[i].getRoleType(), subPlayerVars.get(i));
            } else {
                relationMatchStatement = relationMatchStatement.rel(player.getRoleGetter().getThingGetters()[i].getRoleType(), subPlayerVars.get(i));
            }
        }
        if (relationMatchStatement != null) {
            relationMatchStatement = relationMatchStatement.isa(player.getRoleGetter().getConceptType());
            ArrayList<ThingVariable<?>> playerMatchStatements = new ArrayList<>(subPlayerMatchStatements);
            playerMatchStatements.add(relationMatchStatement);
            if (playerMatchStatements.size() > 1) {
                return playerMatchStatements;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }


    public boolean relationInsertStatementValid(TypeQLInsert insert) {
        if (insert == null) return false;
        if (!insert.toString().contains("isa " + relationConfiguration.getConceptType())) return false;

        for (Configuration.Player player : relationConfiguration.getRequiredNonEmptyPlayers()) {
            if (player.getRoleGetter().getHandler() == TypeHandler.ATTRIBUTE) {
                // the relation must be in the match statement
                if (!insert.toString().contains("isa " + player.getRoleGetter().getConceptType())) return false;
            } else if (player.getRoleGetter().getHandler() == TypeHandler.ENTITY) {
                // the entity must be in the match statement
                if (!insert.toString().contains("isa " + player.getRoleGetter().getConceptType())) return false;
                // each identifying attribute of the entity must be in the match statement
                Configuration.ThingGetter[] ownerships = player.getRoleGetter().getThingGetters();
                for (Configuration.ThingGetter ownership : ownerships) {
                    if (ownership != null) {
                        String conceptType = ownership.getConceptType();
                        if (!insert.toString().contains("has " + conceptType)) return false;
                    }
                }
            } else if (player.getRoleGetter().getHandler() == TypeHandler.RELATION) {
                // the relation must be in the match statement
                if (!insert.toString().contains("isa " + player.getRoleGetter().getConceptType())) return false;
                // each identifying attribute of the relation must be in the match statement
                Configuration.ThingGetter[] ownerships = player.getRoleGetter().getThingGetters();
                for (Configuration.ThingGetter ownership : ownerships) {
                    if (ownership != null && ownership.getHandler() == TypeHandler.OWNERSHIP) {
                        String conceptType = ownership.getConceptType();
                        if (!insert.toString().contains("has " + conceptType)) return false;
                    }
                }
            }
        }

        // all required attributes part of the insert statement
        if (relationConfiguration.getRequireNonEmptyAttributes() != null) {
            for (Configuration.HasAttribute hasAttribute : relationConfiguration.getRequireNonEmptyAttributes()) {
                if (!insert.toString().contains("has " + hasAttribute.getConceptType())) return false;
            }
        }
        // all roles that are required must be in the insert statement
        for (Configuration.Player player : relationConfiguration.getRequiredNonEmptyPlayers()) {
            if (!insert.toString().contains(player.getRoleType() + ":")) return false;
        }
        // must have number of requireNonNull players
        int requiredPlayers = relationConfiguration.getRequiredNonEmptyPlayers().length;
        for (int i = 0; i < requiredPlayers; i++) {
            if (!insert.toString().contains("player-" + i)) return false;
        }

        return true;
    }

    public boolean subEntityValid(ThingVariable.Thing insert, Configuration.ThingGetter thingGetter) {
        if (insert == null) return false;
        if (!insert.toString().contains("isa " + thingGetter.getConceptType())) return false;
        for (Configuration.ThingGetter thingThingGetter : thingGetter.getThingGetters()) {
            if (!insert.toString().contains("has " + thingThingGetter.getConceptType())) return false;
        }
        return true;
    }

    public char getFileSeparator() {
        return this.fileSeparator;
    }

    private TypeHandler getPlayerType(Configuration.Player player) {
        Configuration.RoleGetter playerRoleGetter = player.getRoleGetter();
        if (playerRoleGetter.getHandler() != TypeHandler.OWNERSHIP) {
            return playerRoleGetter.getHandler();
        }
        return null;
    }
}
