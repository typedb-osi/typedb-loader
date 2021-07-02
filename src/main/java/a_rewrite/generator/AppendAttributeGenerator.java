package a_rewrite.generator;

import a_rewrite.config.Configuration;
import a_rewrite.io.ErrorFileLogger;
import a_rewrite.util.GeneratorUtil;
import a_rewrite.util.Util;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typedb.client.common.exception.TypeDBClientException;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.constraint.ThingConstraint;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

public class AppendAttributeGenerator implements Generator {
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");
    private final String filePath;
    private final String[] header;
    private final Configuration.AppendAttribute appendConfiguration;
    private final char fileSeparator;

    public AppendAttributeGenerator(String filePath, Configuration.AppendAttribute appendConfiguration, char fileSeparator) throws IOException {
        this.filePath = filePath;
        this.header = Util.getFileHeader(filePath, fileSeparator);
        this.appendConfiguration = appendConfiguration;
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

        if (appendAttributeInsertStatementValid(statement)) {
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
            ThingVariable.Thing entityMatchStatement = TypeQL.var("thing")
                    .isa(appendConfiguration.getThingGetter().getConceptType());
            for (Configuration.ThingGetter ownershipThingGetter : appendConfiguration.getThingGetter().getThingGetters()) {
                ArrayList<ThingConstraint.Value<?>> constraintValues = GeneratorUtil.generateValueConstraintsConstrainingAttribute(
                        row, header, filePath, fileSeparator, ownershipThingGetter);
                for (ThingConstraint.Value<?> constraintValue : constraintValues) {
                    entityMatchStatement.constrain(GeneratorUtil.valueToHasConstraint(ownershipThingGetter.getConceptType(), constraintValue));
                }
            }

            UnboundVariable insertUnboundVar = TypeQL.var("thing");
            ThingVariable.Thing insertStatement = null;
            for (Configuration.ConstrainingAttribute attributeToAppend : appendConfiguration.getAttributes()) {
                ArrayList<ThingConstraint.Value<?>> constraintValues = GeneratorUtil.generateValueConstraintsConstrainingAttribute(
                        row, header, filePath, fileSeparator, attributeToAppend);
                for (ThingConstraint.Value<?> constraintValue : constraintValues) {
                    if (insertStatement == null) {
                        insertStatement = insertUnboundVar.constrain(GeneratorUtil.valueToHasConstraint(attributeToAppend.getConceptType(), constraintValue));
                    } else {
                        insertStatement.constrain(GeneratorUtil.valueToHasConstraint(attributeToAppend.getConceptType(), constraintValue));
                    }
                }
            }

            if (insertStatement != null) {
                return TypeQL.match(entityMatchStatement).insert(insertStatement);
            } else {
                return TypeQL.insert(TypeQL.var("null").isa("null").has("null", "null"));
            }
        } else {
            return TypeQL.insert(TypeQL.var("null").isa("null").has("null", "null"));
        }
    }

    public boolean appendAttributeInsertStatementValid(TypeQLInsert insert) {
        if (insert == null) return false;
        if (!insert.toString().contains("isa " + appendConfiguration.getThingGetter().getConceptType())) return false;
        for (Configuration.ConstrainingAttribute ownershipThingGetter : appendConfiguration.getThingGetter().getOwnershipThingGetters()) {
            if (!insert.toString().contains(", has " + ownershipThingGetter.getConceptType())) return false;
        }
        if (appendConfiguration.getRequireNonEmptyAttributes() != null) {
            for (Configuration.ConstrainingAttribute constrainingAttribute : appendConfiguration.getRequireNonEmptyAttributes()) {
                if (!insert.toString().contains("has " + constrainingAttribute.getConceptType())) return false;
            }
        }
        return true;
    }

    public char getFileSeparator() {
        return this.fileSeparator;
    }
}
