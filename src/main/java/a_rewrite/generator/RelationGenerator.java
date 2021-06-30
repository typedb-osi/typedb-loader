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
                    boolean isByAttribute = true;
                    if (isByAttribute) {
                        ThingVariable.Thing playerMatchStatement = getThingPlayerMatchStatementByAttribute(row, player, playerVar);
                        if(playerMatchStatement.toString().contains(", has")){
                            playerMatchStatements.add(playerMatchStatement);
                            playerVars.add(playerVar);
                            roleTypes.add(player.getRoleType());
                            playerIdx += 1;
                        }
                    } else {
                        System.out.println("Will do by player(s) in relation");
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

    private ThingVariable.Thing getThingPlayerMatchStatementByAttribute(String[] row, Configuration.Player player, String playerVar) {
        ThingVariable.Thing playerMatchStatement = TypeQL.var(playerVar)
                .isa(player.getRolePlayerGetter().getConceptType());
        for (Configuration.Getter ownershipGetter : player.getOwnershipGetters()) {
            ArrayList<ThingConstraint.Value<?>> tmp = GeneratorUtil.generateValueConstraints(
                    row[GeneratorUtil.getColumnIndexByName(header, ownershipGetter.getColumn())],
                    ownershipGetter.getConceptType(),
                    ownershipGetter.getConceptValueType(),
                    ownershipGetter.getListSeparator(),
                    ownershipGetter.getPreprocessorConfig(),
                    row,
                    filePath,
                    fileSeparator);
            for (ThingConstraint.Value<?> constraintValue : tmp) {
                playerMatchStatement.constrain(GeneratorUtil.valueToHasConstraint(ownershipGetter.getConceptType(), constraintValue));
            }
        }
        return playerMatchStatement;
    }

    private ThingVariable.Attribute getAttributePlayerMatchStatement(String[] row, Configuration.Player player, String playerVar) {
        Configuration.Getter attributeGetter = player.getAttributeGetter();
        ArrayList<ThingConstraint.Value<?>> constraints = GeneratorUtil.generateValueConstraints(
                row[GeneratorUtil.getColumnIndexByName(header, attributeGetter.getColumn())],
                attributeGetter.getConceptType(),
                attributeGetter.getConceptValueType(),
                null,
                attributeGetter.getPreprocessorConfig(),
                row,
                filePath,
                fileSeparator);
        if (constraints.size() > 0) {
            return TypeQL.var(playerVar)
                    .constrain(constraints.get(0))
                    .isa(attributeGetter.getConceptType());
        } else {
            return null;
        }
    }


    public boolean relationInsertStatementValid(TypeQLInsert insert) {
        if (insert == null) return false;
        if (!insert.toString().contains("isa " + relationConfiguration.getConceptType())) return false;

        for (Configuration.Player player : relationConfiguration.getRequiredNonEmptyPlayers()) {
            if (player.getRolePlayerGetter().getHandler() == TypeHandler.ATTRIBUTE) {
                // the relation must be in the match statement
                if (!insert.toString().contains("isa " + player.getRolePlayerGetter().getConceptType())) return false;
            } else if (player.getRolePlayerGetter().getHandler() == TypeHandler.ENTITY) {
                // the entity must be in the match statement
                if (!insert.toString().contains("isa " + player.getRolePlayerGetter().getConceptType())) return false;
                // each identifying attribute of the entity must be in the match statement
                Configuration.Getter[] ownerships = player.getOwnershipGetters();
                for (Configuration.Getter ownership : ownerships) {
                    if (ownership != null) {
                        String conceptType = ownership.getConceptType();
                        if (!insert.toString().contains("has " + conceptType)) return false;
                    }
                }
            } else if (player.getRolePlayerGetter().getHandler() == TypeHandler.RELATION) {
                // the relation must be in the match statement
                if (!insert.toString().contains("isa " + player.getRolePlayerGetter().getConceptType())) return false;
                // each identifying attribute of the relation must be in the match statement
                Configuration.Getter[] ownerships = player.getOwnershipGetters();
                for (Configuration.Getter ownership : ownerships) {
                    if (ownership != null) {
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

    public char getFileSeparator() {
        return this.fileSeparator;
    }

    private TypeHandler getPlayerType(Configuration.Player player) {
        for (Configuration.Getter playerGetter : player.getGetter()) {
            if (playerGetter.getHandler() != TypeHandler.OWNERSHIP) {
                return playerGetter.getHandler();
            }
        }
        return null;
    }
}
