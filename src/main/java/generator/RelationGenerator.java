/*
 * Copyright (C) 2021 Bayer AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package generator;

import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import config.Configuration;
import io.FileLogger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.GeneratorUtil;
import util.Util;
import com.vaticle.typedb.client.api.TypeDBTransaction;
import com.vaticle.typedb.client.common.exception.TypeDBClientException;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.constraint.ThingConstraint;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.util.ArrayList;

import static util.GeneratorUtil.constrainThingWithHasAttributes;
import static util.Util.playerType;

public class RelationGenerator implements Generator {
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.tdl.error");
    private final String filePath;
    private final String[] header;
    private final Configuration.Generator.Relation relationConfiguration;
    private final char fileSeparator;

    public RelationGenerator(String filePath, Configuration.Generator.Relation relationConfiguration, char fileSeparator) throws IOException {
        this.filePath = filePath;
        this.header = Util.getFileHeader(filePath, fileSeparator);
        this.relationConfiguration = relationConfiguration;
        this.fileSeparator = fileSeparator;
    }

    public void write(TypeDBTransaction tx,
                      String[] row) {
        String fileName = FilenameUtils.getName(filePath);
        String fileNoExtension = FilenameUtils.removeExtension(fileName);
        String originalRow = String.join(Character.toString(fileSeparator), row);

        if (row.length > header.length) {
            FileLogger.getLogger().logMalformed(fileName, originalRow);
            dataLogger.error("Malformed Row detected in <" + filePath + "> - written to <" + fileNoExtension + "_malformed.log" + ">");
        }

        TypeQLInsert statement = generateMatchInsertStatement(row);

        if (relationInsertStatementValid(statement)) {
            try {
                tx.query().insert(statement);
            } catch (TypeDBClientException typeDBClientException) {
                FileLogger.getLogger().logUnavailable(fileName, originalRow);
                dataLogger.error("TypeDB Unavailable - Row in <" + filePath + "> not inserted - written to <" + fileNoExtension + "_unavailable.log" + ">");
            }
        } else {
            FileLogger.getLogger().logInvalid(fileName, originalRow);
            dataLogger.error("Invalid Row detected in <" + filePath + "> - written to <" + fileNoExtension + "_invalid.log" + "> - invalid Statement: <" + statement.toString().replace("\n", " ") + ">");
        }
    }

    public TypeQLInsert generateMatchInsertStatement(String[] row) {
        if (row.length > 0) {
            ArrayList<ThingVariable<?>> playerMatchStatements = new ArrayList<>();
            ArrayList<String> playerVars = new ArrayList<>();
            ArrayList<String> roleTypes = new ArrayList<>();

            int playerIdx = 0;
            for (Configuration.Definition.Player player : relationConfiguration.getInsert().getPlayers()) {
                String playerVar = "player-" + playerIdx;

                // ATTRIBUTE PLAYER
                if (playerType(player).equals("attribute")) {
                    ThingVariable.Attribute playerMatchStatement = getAttributePlayerMatchStatement(row, player, playerVar);
                    if (playerMatchStatement != null) {
                        playerMatchStatements.add(playerMatchStatement);
                        playerVars.add(playerVar);
                        roleTypes.add(player.getRole());
                        playerIdx += 1;
                    }
                }

                // ENTITY & RELATION PLAYER BY ATTRIBUTE(s)
                if (playerType(player).equals("byAttribute")) {
                    ThingVariable.Thing playerMatchStatement = getThingPlayerMatchStatementByAttribute(row, player, playerVar);
                    if (playerMatchStatement.toString().contains(", has")) {
                        playerMatchStatements.add(playerMatchStatement);
                        playerVars.add(playerVar);
                        roleTypes.add(player.getRole());
                        playerIdx += 1;
                    }
                }

//                // Relation PLAYER BY PLAYERs
                if (playerType(player).equals("byPlayer")) {
                    ArrayList<ThingVariable<?>> playerMatchStatement = getRelationPlayerMatchStatement(row, player, playerVar);
                    playerMatchStatements.addAll(playerMatchStatement);
                    playerVars.add(playerVar);
                    roleTypes.add(player.getRole());
                    playerIdx += 1;
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
                insertStatement = insertStatement.isa(relationConfiguration.getInsert().getRelation());
                if (relationConfiguration.getInsert().getOwnerships() != null) {
                    constrainThingWithHasAttributes(row, header, filePath, fileSeparator, insertStatement, relationConfiguration.getInsert().getOwnerships());
                }

                return TypeQL.match(playerMatchStatements).insert(insertStatement);
            } else {
                return TypeQL.insert(TypeQL.var("null").isa("null").has("null", "null"));
            }
        } else {
            return TypeQL.insert(TypeQL.var("null").isa("null").has("null", "null"));
        }
    }

    private ThingVariable.Thing getThingPlayerMatchStatementByAttribute(String[] row, Configuration.Definition.Player player, String playerVar) {
        ThingVariable.Thing playerMatchStatement = TypeQL.var(playerVar).isa(player.getMatch().getType());
        for (Configuration.Definition.Attribute consA : player.getMatch().getOwnerships()) {
            ArrayList<ThingConstraint.Value<?>> constraintValues = GeneratorUtil.generateValueConstraintsConstrainingAttribute(
                    row, header, filePath, fileSeparator, consA);
            for (ThingConstraint.Value<?> constraintValue : constraintValues) {
                playerMatchStatement.constrain(GeneratorUtil.valueToHasConstraint(consA.getAttribute(), constraintValue));
            }
        }
        return playerMatchStatement;
    }

    private ThingVariable.Attribute getAttributePlayerMatchStatement(String[] row, Configuration.Definition.Player player, String playerVar) {
        ArrayList<ThingConstraint.Value<?>> constraints = GeneratorUtil.generateValueConstraintsConstrainingAttribute(
                row, header, filePath, fileSeparator, player.getMatch().getAttribute());
        if (constraints.size() > 0) {
            return TypeQL.var(playerVar)
                    .constrain(constraints.get(0))
                    .isa(player.getMatch().getType());
        } else {
            return null;
        }
    }

    private ArrayList<ThingVariable<?>> getRelationPlayerMatchStatement(String[] row, Configuration.Definition.Player player, String playerVar) {
        return recursiveAssemblyMatchStatement(row, player, playerVar);
    }

    private ArrayList<ThingVariable<?>> recursiveAssemblyMatchStatement(String[] row,
                                                                        Configuration.Definition.Player player,
                                                                        String playerVar) {
        if (playerType(player).equals("attribute")) {
            //terminating condition - attribute player:
            ArrayList<ThingVariable<?>> statements = new ArrayList<>();
            ThingVariable<?> statement = getAttributePlayerMatchStatement(row, player, playerVar);
            if (statement != null) {
                statements.add(statement);
            }
            return statements;
        } else if (playerType(player).equals("byAttribute")) {
            //terminating condition - byAttribute player:
            ArrayList<ThingVariable<?>> statements = new ArrayList<>();
            ThingVariable<?> statement = getThingPlayerMatchStatementByAttribute(row, player, playerVar);
            if (statement != null && statement.toString().contains(", has")) {
                statements.add(statement);
            }
            return statements;
        } else if (playerType(player).equals("byPlayer")) {
            // identify relation player "byPlayer"
            ArrayList<ThingVariable<?>> statements = new ArrayList<>();
            //create the relation statement with the player vars that will be filled in recursion:
            UnboundVariable ubv = TypeQL.var(playerVar);
            ThingVariable.Relation relationMatch = null;
            for (int idx = 0; idx < player.getMatch().getPlayers().length; idx++) {
                Configuration.Definition.Player curPlayer = player.getMatch().getPlayers()[idx];
                String curPlayerVar = playerVar + "-" + idx;
                if (idx == 0) {
                    relationMatch = ubv.rel(curPlayer.getRole(), curPlayerVar);
                } else {
                    relationMatch = relationMatch.rel(curPlayer.getRole(), curPlayerVar);
                }
                // this is where the recursion happens to fill the player var!
                ArrayList<ThingVariable<?>> recursiveMatch = recursiveAssemblyMatchStatement(row, curPlayer, curPlayerVar);
                // now add whatever the recursion brought back:
                statements.addAll(recursiveMatch);
            }
            assert relationMatch != null;
            relationMatch = relationMatch.isa(player.getMatch().getType());
            statements.add(relationMatch);

            return statements;
        } else {
            return null;
        }
    }

    public boolean relationInsertStatementValid(TypeQLInsert insert) {
        if (insert == null) return false;
        if (!insert.toString().contains("isa " + relationConfiguration.getInsert().getRelation())) return false;

        int idx = 0;
        for (Configuration.Definition.Player player : relationConfiguration.getInsert().getRequiredPlayers()) {
            // if attribute player
            // the attribute must be in the match statement
            if (playerType(player).equals("attribute")) {
                if (!validateAttributePlayer(player, insert)) {
                    return false;
                }
            } else if (playerType(player).equals("byAttribute")) {
                if (!validateByAttributePlayer(player, insert)) {
                    return false;
                }
            } else if (playerType(player).equals("byPlayer")) {
                if (!recursiveValidationPlayers(player, insert, "player-" + idx)) {
                    return false;
                }
            }
            idx++;
        }

        // all required attributes part of the insert statement
        if (relationConfiguration.getInsert().getRequiredOwnerships() != null) {
            for (Configuration.Definition.Attribute attribute : relationConfiguration.getInsert().getRequiredOwnerships()) {
                if (!insert.toString().contains("has " + attribute.getAttribute())) return false;
            }
        }
        // all roles that are required must be in the insert statement
        for (Configuration.Definition.Player player : relationConfiguration.getInsert().getRequiredPlayers()) {
            if (!insert.toString().contains(player.getRole() + ":")) return false;
        }
        // must have number of requireNonNull players
        int requiredPlayers = relationConfiguration.getInsert().getRequiredPlayers().length;
        for (int i = 0; i < requiredPlayers; i++) {
            if (!insert.toString().contains("player-" + i)) return false;
        }

        return true;
    }

    private boolean validateAttributePlayer(Configuration.Definition.Player player, TypeQLInsert insert) {
        return insert.toString().contains("isa " + player.getMatch().getType());
    }

    private boolean validateByAttributePlayer(Configuration.Definition.Player player, TypeQLInsert insert) {
        if (!insert.toString().contains("isa " + player.getMatch().getType())) return false;
        // each identifying attribute of the entity must be in the match statement
        Configuration.Definition.Attribute[] ownerships = player.getMatch().getOwnerships();
        for (Configuration.Definition.Attribute ownership : ownerships) {
            if (ownership != null) {
                if (!insert.toString().contains("has " + ownership.getAttribute())) return false;
            }
        }
        return true;
    }

    private boolean recursiveValidationPlayers(Configuration.Definition.Player player, TypeQLInsert insert, String playerVar) {

        if (playerType(player).equals("attribute")) {
            //terminating condition - attribute player:
            return validateAttributePlayer(player, insert);
        } else if (playerType(player).equals("byAttribute")) {
            //terminating condition - byAttribute player:
            return validateByAttributePlayer(player, insert);
        } else if (playerType(player).equals("byPlayer")) {
            for (int idx = 0; idx < player.getMatch().getPlayers().length; idx++) {
                // if player is a relation with players - check validity of "shallow" relation player
                Configuration.Definition.Player curPlayer = player.getMatch().getPlayers()[idx];
                String curPlayerVar = playerVar + "-" + idx;
                if (!insert.toString().contains("isa " + curPlayer.getMatch().getType())) return false;
                if (!insert.toString().contains(curPlayer.getRole() + ":")) return false;
                if (!insert.toString().contains(curPlayerVar)) return false;
                recursiveValidationPlayers(curPlayer, insert, curPlayerVar);
            }
            return true;
        } else {
            return false;
        }
    }

    public char getFileSeparator() {
        return this.fileSeparator;
    }
}
