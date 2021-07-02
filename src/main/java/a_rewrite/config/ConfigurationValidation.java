package a_rewrite.config;

import a_rewrite.util.Util;
import com.vaticle.typedb.client.api.answer.ConceptMap;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typedb.client.common.exception.TypeDBClientException;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

public class ConfigurationValidation {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private final Configuration configuration;

    public ConfigurationValidation(Configuration configuration) {
        this.configuration = configuration;
    }

    public void validateConfiguration(HashMap<String, ArrayList<String>> validationReport,
                                      TypeDBSession session) {

        // validate defaultConfig:
        Configuration.DefaultConfig defaultConfig = configuration.getDefaultConfig();
        if (defaultConfig != null) {
            if (defaultConfig.getRowsPerCommit() > 150) {
                validationReport.get("warnings").add("defaultConfig.rowsPerCommit is set to be > 150 - in most cases, choosing a value between 50 and 150 gives the best performance");
            }

            // validate attributes:
            if (configuration.getAttributes() != null) {
                for (Map.Entry<String, Configuration.Attribute> attribute : configuration.getAttributes().entrySet()) {
                    String attributeKey = attribute.getKey();
                    String[] filePaths = attribute.getValue().getDataPaths();
                    Character fileSeparator = Objects.requireNonNullElseGet(attribute.getValue().getConfig().getSeparator(), defaultConfig::getSeparator);
                    String conceptType = attribute.getValue().getAttribute().getConceptType();
                    String column = attribute.getValue().getAttribute().getColumn();
                    String errorBreadcrumbs = ConfigurationHandler.ATTRIBUTES + "." + attributeKey;
                    validateFile(validationReport, errorBreadcrumbs, filePaths, fileSeparator);
                    validateColumnInHeader(validationReport, errorBreadcrumbs, filePaths, column, fileSeparator);
                    validateConceptType(validationReport, session, errorBreadcrumbs, conceptType);
                }
            }

            // validate entities:
            if (configuration.getEntities() != null) {
                for (Map.Entry<String, Configuration.Entity> entity : configuration.getEntities().entrySet()) {
                    String entityKey = entity.getKey();
                    String[] filePaths = entity.getValue().getDataPaths();
                    Character fileSeparator = Objects.requireNonNullElseGet(entity.getValue().getConfig().getSeparator(), defaultConfig::getSeparator);
                    String breadcrumbs = ConfigurationHandler.ENTITIES + "." + entityKey;
                    validateFile(validationReport, breadcrumbs, filePaths, fileSeparator);
                    String conceptType = entity.getValue().getConceptType();
                    validateConceptType(validationReport, session, breadcrumbs, conceptType);
                    validateEntityHasAttributes(validationReport, session, entity.getValue().getAttributes(), breadcrumbs, filePaths, fileSeparator);
                }
            }

            // validate relations:
//            if (configuration.getRelations() != null) {
//                for (Map.Entry<String, Configuration.Relation> relation : configuration.getRelations().entrySet()) {
//                    String relationKey = relation.getKey();
//                    String[] filePaths = relation.getValue().getDataPaths();
//                    Character fileSeparator = Objects.requireNonNullElseGet(relation.getValue().getSeparator(), defaultConfig::getSeparator);
//                    String breadcrumbs = ConfigurationHandler.RELATIONS + "." + relationKey;
//                    validateFile(validationReport, breadcrumbs, filePaths, fileSeparator);
//                    String conceptType = relation.getValue().getConceptType();
//                    validateConceptType(validationReport, session, breadcrumbs, conceptType);
//                    validateRelationHasAttributes(validationReport, session, relation.getValue().getAttributes(), breadcrumbs, filePaths, fileSeparator);
//                    validateRelationPlayers(validationReport, session, conceptType, relation.getValue().getPlayers(), breadcrumbs, filePaths, fileSeparator);
//                }
//            }

        } else {
            validationReport.get("errors").add("defaultConfig does not exist");
        }
    }

    public void validateSchemaPresent(HashMap<String, ArrayList<String>> validationReport) {
        String schemaPath = configuration.getDefaultConfig().getSchemaPath();
        if (schemaPath == null) {
            validationReport.get("errors").add("defaultConfig.schemaPath: missing required field");
        } else {
            try {
                Util.newBufferedReader(schemaPath);
            } catch (FileNotFoundException fileNotFoundException) {
                validationReport.get("errors").add("defaultConfig.schemaPath - schema file not found under: <" + schemaPath + ">");
            }
        }
    }

    private void validateFile(HashMap<String, ArrayList<String>> validationReport,
                              String breadcrumbs,
                              String[] filepaths,
                              Character fileSeparator) {
        for (String filepath : filepaths) {
            try {
                Util.getFileHeader(filepath, fileSeparator);
            } catch (IOException fileNotFoundException) {
                validationReport.get("errors").add(breadcrumbs + ".dataPath: <" + filepath + ">: file not found");
            } catch (IllegalArgumentException ioException) {
                validationReport.get("errors").add(breadcrumbs + ".dataPath: <" + filepath + ">: file is empty");
            }
        }
    }

    private void validateColumnInHeader(HashMap<String, ArrayList<String>> validationReport,
                                        String breadcrumbs,
                                        String[] filepaths,
                                        String column,
                                        Character fileSeparator) {
        for (String filepath : filepaths) {
            try {
                String[] header = Util.getFileHeader(filepath, fileSeparator);
                if (Arrays.stream(header).noneMatch(headerColumn -> headerColumn.equals(column))) {
                    validationReport.get("errors").add(breadcrumbs + ".column: <" + column + "> column not found in header of file <" + filepath + ">");
                }
            } catch (IOException | IllegalArgumentException ignored) {
            }
        }
    }

    private void validateConceptType(HashMap<String, ArrayList<String>> validationReport,
                                     TypeDBSession session,
                                     String breadcrumbs,
                                     String conceptType) {
        TypeDBTransaction txn = session.transaction(TypeDBTransaction.Type.READ);
        TypeQLMatch query = TypeQL.match(TypeQL.var("t").type(conceptType));
        try {
            appLogger.trace(txn.query().match(query).count());
            txn.close();
        } catch (TypeDBClientException typeDBClientException) {
            if (typeDBClientException.toString().contains("Invalid Type Read: The type '" + conceptType + "' does not exist.")) {
                validationReport.get("errors").add(breadcrumbs + ".conceptType: <" + conceptType + "> does not exist in schema");
            } else {
                throw typeDBClientException;
            }
        }
    }

    private void validateEntityHasAttributes(HashMap<String, ArrayList<String>> validationReport,
                                             TypeDBSession session,
                                             Configuration.ConstrainingAttribute[] constrainingAttributes,
                                             String breadcrumbs,
                                             String[] filePaths,
                                             Character fileSeparator) {
        if (constrainingAttributes != null) {
            validateHasAttributes(validationReport, session, constrainingAttributes, breadcrumbs, filePaths, fileSeparator);
        } else {
            validationReport.get("errors").add(breadcrumbs + ".attributes: missing required attributes block");
        }
    }

    private void validateRelationHasAttributes(HashMap<String, ArrayList<String>> validationReport,
                                               TypeDBSession session,
                                               Configuration.ConstrainingAttribute[] constrainingAttributes,
                                               String breadcrumbs,
                                               String[] filePaths,
                                               Character fileSeparator) {
        if (constrainingAttributes != null) {
            validateHasAttributes(validationReport, session, constrainingAttributes, breadcrumbs, filePaths, fileSeparator);
        }
    }

    private void validateHasAttributes(HashMap<String, ArrayList<String>> validationReport,
                                       TypeDBSession session,
                                       Configuration.ConstrainingAttribute[] constrainingAttributes,
                                       String breadcrumbs,
                                       String[] filePaths,
                                       Character fileSeparator) {
        int entryIdx = 0;
        for (Configuration.ConstrainingAttribute attribute : constrainingAttributes) {
            String aBreadcrumbs = breadcrumbs + ".attributes.[" + entryIdx + "]";

            if (attribute.getColumn() != null) {
                validateColumnInHeader(validationReport, aBreadcrumbs, filePaths, attribute.getColumn(), fileSeparator);
            } else {
                validationReport.get("errors").add(aBreadcrumbs + ".column: missing required field");
            }

            if (attribute.getConceptType() != null) {
                validateConceptType(validationReport, session, aBreadcrumbs, attribute.getConceptType());
            } else {
                validationReport.get("errors").add(aBreadcrumbs + ".conceptType: missing required field");
            }

            if (attribute.getRequireNonEmpty() == null) {
                validationReport.get("warnings").add(aBreadcrumbs + ".requireNonEmpty: field not set - defaults to false");
            }
            entryIdx += 1;
        }
    }

    private void validateRelationPlayers(HashMap<String, ArrayList<String>> validationReport,
                                         TypeDBSession session,
                                         String relationConceptType,
                                         Configuration.Player[] players,
                                         String breadcrumbs,
                                         String[] filePaths,
                                         Character fileSeparator) {
        int playerIdx = 0;
        for (Configuration.Player player : players) {
            String pBreadcrumbs = breadcrumbs + ".players.[" + playerIdx + "]";
            if (player.getRoleType() != null) {
                validateRoleType(validationReport, session, pBreadcrumbs, relationConceptType, player.getRoleType());
            } else {
                validationReport.get("errors").add(pBreadcrumbs + ".roleType: missing required field");
            }

            if (player.getRequireNonEmpty() == null) {
                validationReport.get("warnings").add(pBreadcrumbs + ".requireNonEmpty: field not set - defaults to false");
            }

            if (player.getRoleGetter() != null) {
                validateGetterComposition(validationReport, player.getRoleGetter(), pBreadcrumbs);

                int getterIdx = 0;
                Configuration.RoleGetter roleGetter = player.getRoleGetter();
                String gBreadcrumbs = pBreadcrumbs + ".roleGetter";

                if (roleGetter.getHandler() == null) {
                    validationReport.get("errors").add(gBreadcrumbs + ".handler: missing required field (one of [entity, relation, attribute, ownership])");
                }
                if (roleGetter.getConceptType() == null) {
                    validationReport.get("errors").add(gBreadcrumbs + ".conceptType: missing required field");
                } else {
                    validateConceptType(validationReport, session, gBreadcrumbs, roleGetter.getConceptType());
                }

                if (roleGetter.getColumn() != null) {
                    validateColumnInHeader(validationReport, gBreadcrumbs, filePaths, roleGetter.getColumn(), fileSeparator);
                }
            } else {
                validationReport.get("errors").add(pBreadcrumbs + ".players: missing required players block");
            }

            playerIdx += 1;
        }
    }

    private void validateRoleType(HashMap<String, ArrayList<String>> validationReport,
                                  TypeDBSession session,
                                  String breadcrumbs,
                                  String relationConceptType,
                                  String roleType) {
        TypeDBTransaction txn = session.transaction(TypeDBTransaction.Type.READ);
        TypeQLMatch query = TypeQL.match(TypeQL.type(relationConceptType).relates(TypeQL.var("r"))).get("r");
        Stream<ConceptMap> answers = txn.query().match(query);
        if (answers.noneMatch(a -> a.get("r").asRoleType().getLabel().name().equals(roleType))) {
            validationReport.get("errors").add(breadcrumbs + ".roleType: <" + roleType + "> is not a role for relation of type <" + relationConceptType + "> in schema");
        }
        txn.close();
    }

    private void validateGetterComposition(HashMap<String, ArrayList<String>> validationReport,
                                           Configuration.RoleGetter roleGetter,
                                           String breadcrumbs) {
        int attributeCounter = 0;
        int entityCounter = 0;
        int relationCounter = 0;
        int ownershipCounter = 0;


        if (roleGetter.getHandler().equals(TypeHandler.ATTRIBUTE)) {
            attributeCounter += 1;
        }
        if (roleGetter.getHandler().equals(TypeHandler.ENTITY)) {
            entityCounter += 1;
        }
        if (roleGetter.getHandler().equals(TypeHandler.RELATION)) {
            relationCounter += 1;
        }
        if (roleGetter.getHandler().equals(TypeHandler.OWNERSHIP)) {
            ownershipCounter += 1;
        }

        int typeHandlerSum = attributeCounter + entityCounter + relationCounter;

        if (typeHandlerSum != 1) {
            validationReport.get("errors").add(breadcrumbs + ".getter: each getter block must contain exactly one getter with handler <attribute>, <entity>, or <relation>");
        } else {
            if (attributeCounter == 1) {
                if (ownershipCounter > 0) {
                    validationReport.get("errors").add(breadcrumbs + ".getter: each getter block with an <attribute> must not contain any other getters of any other type");
                }
            } else {
                if (ownershipCounter < 1) {
                    validationReport.get("errors").add(breadcrumbs + ".getter: each getter block with an <entity> or <relation> must contain one or more getters with handler <ownership>");
                }
            }
        }
    }

//    private TypeHandler getPlayerType(Configuration.Player player) {
//        int entityGetter = 0;
//        int attributeGetter = 0;
//        int relationGetter = 0;
//
//        for (Configuration.Getter playerGetter : player.getGetter()) {
//            switch (playerGetter.getHandler().toString()) {
//                case "attribute":
//                    attributeGetter++; break;
//                case "entity":
//                    entityGetter++; break;
//                case "relation":
//                    relationGetter++; break;
//            }
//        }
//
//        if (entityGetter + attributeGetter + relationGetter == 1) {
//            if (attributeGetter == 1) {
//                return TypeHandler.ATTRIBUTE;
//            } else if (entityGetter == 1) {
//                return TypeHandler.ENTITY;
//            } else if (relationGetter == 1) {
//                return TypeHandler.RELATION;
//            } else {
//                return null;
//            }
//        } else {
//            return null;
//        }
//    }

}
