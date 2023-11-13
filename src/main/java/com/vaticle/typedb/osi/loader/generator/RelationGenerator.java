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

package com.vaticle.typedb.osi.loader.generator;

import com.vaticle.typedb.driver.api.TypeDBTransaction;
import com.vaticle.typedb.driver.api.answer.ConceptMap;
import com.vaticle.typedb.driver.common.exception.TypeDBDriverException;
import com.vaticle.typedb.osi.loader.config.Configuration;
import com.vaticle.typedb.osi.loader.io.FileLogger;
import com.vaticle.typedb.osi.loader.util.GeneratorUtil;
import com.vaticle.typedb.osi.loader.util.TypeDBUtil;
import com.vaticle.typedb.osi.loader.util.Util;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.builder.ConceptVariableBuilder;
import com.vaticle.typeql.lang.pattern.constraint.ThingConstraint;
import com.vaticle.typeql.lang.pattern.statement.ThingStatement;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static com.vaticle.typedb.osi.loader.util.GeneratorUtil.constrainThingWithHasAttributes;
import static com.vaticle.typedb.osi.loader.util.TypeDBUtil.safeInsert;

public class RelationGenerator implements Generator {
    private static final Logger dataLogger = LogManager.getLogger("com.vaticle.typedb.osi.loader.error");
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

    @Override
    public void write(TypeDBTransaction tx, String[] row, boolean allowMultiInsert) {
        String fileName = FilenameUtils.getName(filePath);
        String fileNoExtension = FilenameUtils.removeExtension(fileName);
        String originalRow = String.join(Character.toString(fileSeparator), row);

        if (row.length > header.length) {
            FileLogger.getLogger().logMalformed(fileName, originalRow);
            dataLogger.error("Malformed Row detected in <" + filePath + "> - written to <" + fileNoExtension + "_malformed.log" + ">");
        }

        TypeQLInsert query = generateMatchInsertStatement(row);

        if (relationInsertStatementValid(query)) {
            try {
                Iterator<ConceptMap> answers = TypeDBUtil.executeMatch(tx, query);
                if (!answers.hasNext()) {
                    FileLogger.getLogger().logNoMatches(fileName, originalRow);
                    dataLogger.error("Match-insert failed - File <" + filePath + "> row <" + originalRow + "> generates query <" + query + "> which matched no answers.");
                } else {
                    safeInsert(tx, query, answers, allowMultiInsert, filePath, originalRow, dataLogger);
                }
            } catch (TypeDBDriverException typeDBDriverException) {
                FileLogger.getLogger().logUnavailable(fileName, originalRow);
                dataLogger.error("TypeDB Unavailable - Row in <" + filePath + "> not inserted - written to <" + fileNoExtension + "_unavailable.log" + ">");
            }
        } else {
            FileLogger.getLogger().logInvalid(fileName, originalRow);
            dataLogger.error("Invalid Row detected in <" + filePath + "> - written to <" + fileNoExtension + "_invalid.log" + "> - invalid Statement: <" + query.toString().replace("\n", " ") + ">");
        }
    }

    public TypeQLInsert generateMatchInsertStatement(String[] row) {
        if (row.length > 0) {
            ArrayList<ThingStatement<?>> playerMatchStatements = new ArrayList<>();
            ArrayList<String> playerVars = new ArrayList<>();
            ArrayList<String> roleTypes = new ArrayList<>();

            int playerIdx = 0;
            for (Configuration.Definition.Player player : relationConfiguration.getInsert().getPlayers()) {
                String playerVar = "player-" + playerIdx;

                // ATTRIBUTE PLAYER
                if (Util.playerType(player).equals("attribute")) {
                    ThingStatement.Attribute playerMatchStatement = getAttributePlayerMatchStatement(row, player, playerVar);
                    if (playerMatchStatement != null) {
                        playerMatchStatements.add(playerMatchStatement);
                        playerVars.add(playerVar);
                        roleTypes.add(player.getRole());
                        playerIdx += 1;
                    }
                }

                // ENTITY & RELATION PLAYER BY ATTRIBUTE(s)
                if (Util.playerType(player).equals("byAttribute")) {
                    ThingStatement.Thing playerMatchStatement = getThingPlayerMatchStatementByAttribute(row, player, playerVar);
                    if (playerMatchStatement.constraints().stream().anyMatch(ThingConstraint::isHas)) {
                        playerMatchStatements.add(playerMatchStatement);
                        playerVars.add(playerVar);
                        roleTypes.add(player.getRole());
                        playerIdx += 1;
                    }
                }

//                // Relation PLAYER BY PLAYERs
                if (Util.playerType(player).equals("byPlayer")) {
                    ArrayList<ThingStatement<?>> playerMatchStatement = getRelationPlayerMatchStatement(row, player, playerVar);
                    playerMatchStatements.addAll(playerMatchStatement);
                    playerVars.add(playerVar);
                    roleTypes.add(player.getRole());
                    playerIdx += 1;
                }

            }

            ThingStatement.Relation insertStatement = null;
            for (int i = 0; i < roleTypes.size(); i++) {
                if (insertStatement == null) {
                    insertStatement = TypeQL.cVar("rel").rel(roleTypes.get(i), TypeQL.cVar(playerVars.get(i)));
                } else {
                    insertStatement = insertStatement.rel(roleTypes.get(i), TypeQL.cVar(playerVars.get(i)));
                }
            }
            if (insertStatement != null) {
                insertStatement = insertStatement.isa(relationConfiguration.getInsert().getRelation());
                if (relationConfiguration.getInsert().getOwnerships() != null) {
                    constrainThingWithHasAttributes(row, header, filePath, fileSeparator, insertStatement, relationConfiguration.getInsert().getOwnerships());
                }

                return TypeQL.match(playerMatchStatements).insert(insertStatement);
            } else {
                return TypeQL.insert(TypeQL.cVar("null").isa("null").has("null", "null"));
            }
        } else {
            return TypeQL.insert(TypeQL.cVar("null").isa("null").has("null", "null"));
        }
    }

    private ThingStatement.Thing getThingPlayerMatchStatementByAttribute(String[] row, Configuration.Definition.Player player, String playerVar) {
        ThingStatement.Thing playerMatchStatement = TypeQL.cVar(playerVar).isa(player.getMatch().getType());
        for (Configuration.Definition.Attribute consA : player.getMatch().getOwnerships()) {
            ArrayList<ThingConstraint.Predicate> constraintValues = GeneratorUtil.generateValueConstraintsConstrainingAttribute(
                    row, header, filePath, fileSeparator, consA);
            for (ThingConstraint.Predicate constraintValue : constraintValues) {
                playerMatchStatement.constrain(GeneratorUtil.valueToHasConstraint(consA.getAttribute(), constraintValue));
            }
        }
        return playerMatchStatement;
    }

    private ThingStatement.Attribute getAttributePlayerMatchStatement(String[] row, Configuration.Definition.Player player, String playerVar) {
        ArrayList<ThingConstraint.Predicate> constraints = GeneratorUtil.generateValueConstraintsConstrainingAttribute(
                row, header, filePath, fileSeparator, player.getMatch().getAttribute());
        if (constraints.size() > 0) {
            return TypeQL.cVar(playerVar)
                    .constrain(constraints.get(0))
                    .isa(player.getMatch().getType());
        } else {
            return null;
        }
    }

    private ArrayList<ThingStatement<?>> getRelationPlayerMatchStatement(String[] row, Configuration.Definition.Player player, String playerVar) {
        return recursiveAssemblyMatchStatement(row, player, playerVar);
    }

    private ArrayList<ThingStatement<?>> recursiveAssemblyMatchStatement(String[] row,
                                                                        Configuration.Definition.Player player,
                                                                        String playerVar) {
        if (Util.playerType(player).equals("attribute")) {
            //terminating condition - attribute player:
            ArrayList<ThingStatement<?>> statements = new ArrayList<>();
            ThingStatement<?> statement = getAttributePlayerMatchStatement(row, player, playerVar);
            if (statement != null) {
                statements.add(statement);
            }
            return statements;
        } else if (Util.playerType(player).equals("byAttribute")) {
            //terminating condition - byAttribute player:
            ArrayList<ThingStatement<?>> statements = new ArrayList<>();
            ThingStatement<?> statement = getThingPlayerMatchStatementByAttribute(row, player, playerVar);
            if (statement != null && statement.constraints().stream().anyMatch(ThingConstraint::isHas)) {
                statements.add(statement);
            }
            return statements;
        } else if (Util.playerType(player).equals("byPlayer")) {
            // identify relation player "byPlayer"
            ArrayList<ThingStatement<?>> statements = new ArrayList<>();
            //create the relation statement with the player vars that will be filled in recursion:
            ConceptVariableBuilder ubv = TypeQL.cVar(playerVar);
            ThingStatement.Relation relationMatch = null;
            for (int idx = 0; idx < player.getMatch().getPlayers().length; idx++) {
                Configuration.Definition.Player curPlayer = player.getMatch().getPlayers()[idx];
                String curPlayerVar = playerVar + "-" + idx;
                if (idx == 0) {
                    relationMatch = ubv.rel(curPlayer.getRole(), TypeQL.cVar(curPlayerVar));
                } else {
                    relationMatch = relationMatch.rel(curPlayer.getRole(), TypeQL.cVar(curPlayerVar));
                }
                // this is where the recursion happens to fill the player var!
                ArrayList<ThingStatement<?>> recursiveMatch = recursiveAssemblyMatchStatement(row, curPlayer, curPlayerVar);
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
            if (Util.playerType(player).equals("attribute")) {
                if (!validateAttributePlayer(player, insert)) {
                    return false;
                }
            } else if (Util.playerType(player).equals("byAttribute")) {
                if (!validateByAttributePlayer(player, insert)) {
                    return false;
                }
            } else if (Util.playerType(player).equals("byPlayer")) {
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

        if (Util.playerType(player).equals("attribute")) {
            //terminating condition - attribute player:
            return validateAttributePlayer(player, insert);
        } else if (Util.playerType(player).equals("byAttribute")) {
            //terminating condition - byAttribute player:
            return validateByAttributePlayer(player, insert);
        } else if (Util.playerType(player).equals("byPlayer")) {
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
