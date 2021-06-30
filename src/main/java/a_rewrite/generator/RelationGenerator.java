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
        ArrayList<ThingVariable.Thing> playerMatchStatements = new ArrayList<>();
        //TODO: replace this String solution with language builder
        ArrayList<String> playerVars = new ArrayList<>();
        ArrayList<String> roleTypes = new ArrayList<>();

        int playerIdx = 0;
        for (Configuration.Player player : relationConfiguration.getPlayers()) {
            String playerVar = "player-" + playerIdx;
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
            if(playerMatchStatement.toString().contains(", has")){
                playerMatchStatements.add(playerMatchStatement);
                playerVars.add(playerVar);
                roleTypes.add(player.getRoleType());
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
        insertStatement = insertStatement.isa(relationConfiguration.getConceptType());

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

        return TypeQL.match(playerMatchStatements).insert(insertStatement);
    }

    public boolean relationInsertStatementValid(TypeQLInsert insert) {
        if (insert == null) return false;
        if (!insert.toString().contains("isa " + relationConfiguration.getConceptType())) return false;

        for (Configuration.Player player : relationConfiguration.getRequiredNonEmptyPlayers()) {
            if (player.getRolePlayerGetter().getHandler() == TypeHandler.ATTRIBUTE) {
                // the relation must be in the match statement
                if (!insert.toString().contains(player.getRolePlayerGetter().getConceptType())) return false;
                // TODO
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
                // TODO
            }
        }

        // all required attributes part of the insert statement
        for (Configuration.HasAttribute hasAttribute : relationConfiguration.getRequireNonEmptyAttributes()) {
            if (!insert.toString().contains("has " + hasAttribute.getConceptType())) return false;
        }
        return true;
    }

    public char getFileSeparator() {
        return this.fileSeparator;
    }
}
