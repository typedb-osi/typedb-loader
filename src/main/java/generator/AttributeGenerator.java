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
import com.vaticle.typeql.lang.pattern.constraint.ThingConstraint;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AttributeGenerator implements Generator {
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.tdl.error");
    private final String filePath;
    private final String[] header;
    private final Configuration.Attribute attributeConfiguration;
    private final char fileSeparator;

    public AttributeGenerator(String filePath, Configuration.Attribute attributeConfiguration, char fileSeparator) throws IOException {
        this.filePath = filePath;
        this.header = Util.getFileHeader(filePath, fileSeparator);
        this.attributeConfiguration = attributeConfiguration;
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

        for (TypeQLInsert statement : generateInsertStatements(row)) {
            if (isValid(statement)) {
                try {
                    tx.query().insert(statement);
                } catch (TypeDBClientException graknClientException) {
                    FileLogger.getLogger().logUnavailable(fileName, originalRow);
                    dataLogger.error("TypeDB Unavailable - Row in <" + filePath + "> not inserted - written to <" + fileNoExtension + "_unavailable.log" + ">");
                }
            } else {
                FileLogger.getLogger().logInvalid(fileName, originalRow);
                dataLogger.error("Invalid Row detected in <" + filePath + "> - written to <" + fileNoExtension + "_invalid.log" + "> - invalid Statement: <" + statement.toString().replace("\n", " ") + ">");
            }
        }
    }

    public List<TypeQLInsert> generateInsertStatements(String[] row) {
        if (row.length > 0) {
            ArrayList<ThingConstraint.Value<?>> constraints = GeneratorUtil.generateValueConstraintsConstrainingAttribute(
                    row, header, filePath, fileSeparator, attributeConfiguration.getAttribute());

            List<TypeQLInsert> insertStatements = new ArrayList<>();
            for (ThingConstraint.Value<?> constraint : constraints) {
                insertStatements.add(TypeQL.insert(
                        TypeQL.var("a")
                                .constrain(constraint)
                                .isa(attributeConfiguration.getAttribute().getConceptType())
                ));
            }
            return insertStatements;
        } else {
            return List.of(TypeQL.insert(TypeQL.var("null").isa("null").has("null", "null")));
        }

    }

    private boolean isValid(TypeQLInsert insert) {
        return insert.toString().contains("isa " + attributeConfiguration.getAttribute().getConceptType());
    }

    public char getFileSeparator() {
        return this.fileSeparator;
    }
}
