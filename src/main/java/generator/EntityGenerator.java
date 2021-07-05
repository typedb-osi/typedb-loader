package generator;

import config.Configuration;
import io.FileLogger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.GeneratorUtil;
import util.Util;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typedb.client.common.exception.TypeDBClientException;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;

import static util.GeneratorUtil.constrainThingWithHasAttributes;

public class EntityGenerator implements Generator {
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.tbl.error");
    private final String filePath;
    private final String[] header;
    private final Configuration.Entity entityConfiguration;
    private final char fileSeparator;

    public EntityGenerator(String filePath, Configuration.Entity entityConfiguration, char fileSeparator) throws IOException {
        this.filePath = filePath;
        this.header = Util.getFileHeader(filePath, fileSeparator);
        this.entityConfiguration = entityConfiguration;
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

        TypeQLInsert statement = generateThingInsertStatement(row);
        if (entityInsertStatementValid(statement)) {
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

    public TypeQLInsert generateThingInsertStatement(String[] row) {
        if (row.length > 0) {
            ThingVariable.Thing insertStatement = GeneratorUtil.generateBoundThingVar(entityConfiguration.getConceptType());

            constrainThingWithHasAttributes(row, header, filePath, fileSeparator, insertStatement, entityConfiguration.getAttributes());

            return TypeQL.insert(insertStatement);
        } else {
            return TypeQL.insert(TypeQL.var("null").isa("null").has("null", "null"));
        }
    }

    public boolean entityInsertStatementValid(TypeQLInsert insert) {
        if (insert == null) return false;
        if (!insert.toString().contains("isa " + entityConfiguration.getConceptType())) return false;
        for (Configuration.ConstrainingAttribute constrainingAttribute : entityConfiguration.getRequireNonEmptyAttributes()) {
            if (!insert.toString().contains("has " + constrainingAttribute.getConceptType())) return false;
        }
        return true;
    }

    public char getFileSeparator() {
        return this.fileSeparator;
    }
}
