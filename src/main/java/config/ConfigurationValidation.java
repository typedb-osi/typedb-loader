package config;

import util.Util;
import com.vaticle.typedb.client.api.answer.ConceptMap;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typedb.client.common.exception.TypeDBClientException;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

import static util.Util.getRowsPerCommit;
import static util.Util.getSeparator;

public class ConfigurationValidation {

    private final Configuration configuration;

    public ConfigurationValidation(Configuration configuration) {
        this.configuration = configuration;
    }

    public void validateConfiguration(HashMap<String, ArrayList<String>> validationReport,
                                      TypeDBSession session) {

        // DEFAULT CONFIG (note: schema will be validated beforehand in TypeDBLoader.java because need migrated schema for validation that follows):
        Configuration.GlobalConfig globalConfig = configuration.getGlobalConfig();
        if (globalConfig != null) {
            //ROWS PER COMMIT
            if (globalConfig.getRowsPerCommit() != null) {
                if (globalConfig.getRowsPerCommit() > 150) {
                    validationReport.get("warnings").add("defaultConfig.rowsPerCommit is set to be > 150 - in most cases, choosing a value between 50 and 150 gives the best performance");
                }
            } else {
                validationReport.get("warnings").add("defaultConfig.rowsPerCommit is not set - it must therefore be set individually for each generator");
            }
            //SEPARATOR
            if (globalConfig.getSeparator() == null) {
                validationReport.get("warnings").add("defaultConfig.separator is not set - it must therefore be set individually for each generator");
            }
            //IGNORE_GENERATORS
            if (globalConfig.getIgnoreGenerators() != null) {
                validationReport.get("warnings").add("defaultConfig.ignoreGenerators: ignoring generators: [" + String.join(", ", globalConfig.getIgnoreGenerators()) + "]");
            }
        } else {
            validationReport.get("errors").add("defaultConfig does not exist");
        }

        // ATTRIBUTES:
        if (configuration.getAttributes() != null) {
            for (Map.Entry<String, Configuration.Attribute> attribute : configuration.getAttributes().entrySet()) {
                // Breadcrumbs
                String breadcrumbs = ConfigurationHandler.ATTRIBUTES + "." + attribute.getKey();
                // CONFIG
                boolean gc = valGeneratorConfig(validationReport, breadcrumbs, configuration, attribute.getValue().getConfig());
                // DATAPATHS
                boolean dps = valDataPaths(validationReport, breadcrumbs, attribute.getValue());
                // ATTRIBUTE
                if (gc && dps) {
                    valAttributeAttribute(validationReport, breadcrumbs, configuration, attribute.getValue(), attribute.getValue().getAttribute(), session);
                }
            }
        }

        // validate entities:
        if (configuration.getEntities() != null) {
            for (Map.Entry<String, Configuration.Entity> entity : configuration.getEntities().entrySet()) {
                // Breadcrumbs
                String breadcrumbs = ConfigurationHandler.ENTITIES + "." + entity.getKey();
                // CONFIG
                boolean gc = valGeneratorConfig(validationReport, breadcrumbs, configuration, entity.getValue().getConfig());
                // DATAPATHS
                boolean dps = valDataPaths(validationReport, breadcrumbs, entity.getValue());
                // ENTITY
                if (gc && dps) {
                    String conceptType = entity.getValue().getConceptType();
                    boolean entityExists = valConceptTypeInSchema(validationReport, session, breadcrumbs, conceptType);
                    if (entityExists) {
                        valEntityHasAttributes(validationReport, breadcrumbs, configuration, entity.getValue(), entity.getValue().getAttributes(), session);
                    }
                }
            }
        }

        // validate relations:

        if (configuration.getRelations() != null) {
            for (Map.Entry<String, Configuration.Relation> relation : configuration.getRelations().entrySet()) {
                // Breadcrumbs
                String breadcrumbs = ConfigurationHandler.RELATIONS + "." + relation.getKey();
                // CONFIG
                boolean gc = valGeneratorConfig(validationReport, breadcrumbs, configuration, relation.getValue().getConfig());
                // DATAPATHS
                boolean dps = valDataPaths(validationReport, breadcrumbs, relation.getValue());
                // RELATION
                if (gc && dps) {
                    String conceptType = relation.getValue().getConceptType();

                    boolean relationExists = valConceptTypeInSchema(validationReport, session, breadcrumbs, conceptType);
                    if (relationExists) {
                        valRelationHasAttributes(validationReport, breadcrumbs, configuration, relation.getValue(), relation.getValue().getAttributes(), session);
                        valRelationPlayers(validationReport, breadcrumbs, configuration, relation.getValue(), session);
                    }
                }
            }
        }
        System.out.println("rel end");

        // validate appendAttribute:
        if (configuration.getAppendAttribute() != null) {
            for (Map.Entry<String, Configuration.AppendAttribute> appendAttribute : configuration.getAppendAttribute().entrySet()) {
                // Breadcrumbs
//                String breadcrumbs = ConfigurationHandler.APPEND_ATTRIBUTE + "." + appendAttribute.getKey();
//                // CONFIG
//                boolean gc = valGeneratorConfig(validationReport, breadcrumbs, configuration, appendAttribute.getValue().getConfig());
//                // DATAPATHS
//                boolean dps = valDataPaths(validationReport, breadcrumbs, appendAttribute.getValue());
//                // APPEND_ATTRIBUTE
//                if (gc && dps) {
//                    String conceptType = appendAttribute.getValue().getConceptType();
//                    boolean relationExists = valConceptTypeInSchema(validationReport, session, breadcrumbs, conceptType);
//                    if (relationExists) {
//                        valRelationHasAttributes(validationReport, breadcrumbs, configuration, relation.getValue(), relation.getValue().getAttributes(), session);
//                        valRelationPlayers(validationReport, breadcrumbs, configuration, relation.getValue(), session);
//                    }
//                }
            }
        }

        // validate appendOrInsertAttribute:
        if (configuration.getAppendAttributeOrInsertThing() != null) {
            for (Map.Entry<String, Configuration.AppendAttributeOrInsertThing> appendOrInsertAttribute : configuration.getAppendAttributeOrInsertThing().entrySet()) {
                // Breadcrumbs
                String breadcrumbs = ConfigurationHandler.APPEND_ATTRIBUTE_OR_INSERT_THING + "." + appendOrInsertAttribute.getKey();
                // CONFIG
                boolean gc = valGeneratorConfig(validationReport, breadcrumbs, configuration, appendOrInsertAttribute.getValue().getConfig());
                // DATAPATHS
                boolean dps = valDataPaths(validationReport, breadcrumbs, appendOrInsertAttribute.getValue());
            }
        }
    }

    public void valAttributeAttribute(HashMap<String, ArrayList<String>> validationReport,
                                      String breadcrumbs,
                                      Configuration configuration,
                                      Configuration.Generator generator,
                                      Configuration.ConstrainingAttribute attribute,
                                      TypeDBSession session) {
        breadcrumbs = breadcrumbs + ".attribute";
        if (attribute == null) {
            validationReport.get("error").add(breadcrumbs + ": missing required object (note: not <.attributes> but <.attribute>)");
        } else {
            valConstrainingAttribute(validationReport, breadcrumbs, configuration, generator, attribute, session);
        }

    }

    public void valConstrainingAttribute(HashMap<String, ArrayList<String>> validationReport,
                                         String breadcrumbs,
                                         Configuration configuration,
                                         Configuration.Generator generator,
                                         Configuration.ConstrainingAttribute attribute,
                                         TypeDBSession session) {
        if (attribute.getConceptType() == null) {
            validationReport.get("error").add(breadcrumbs + ".conceptType: missing required field");
        } else {
            valConceptTypeInSchema(validationReport, session, breadcrumbs, attribute.getConceptType());
        }
        if (attribute.getColumn() == null) {
            validationReport.get("error").add(breadcrumbs + ".column: missing required field");
        } else {
            valColumnInHeader(validationReport, breadcrumbs, configuration, generator, attribute.getColumn());
        }
        if (attribute.getRequireNonEmpty() == null) {
            validationReport.get("warnings").add(breadcrumbs + ".requireNonEmpty: field not set - defaults to false");
        }
    }


    public boolean valGeneratorConfig(HashMap<String, ArrayList<String>> validationReport,
                                      String breadcrumbs,
                                      Configuration dc,
                                      Configuration.GeneratorConfig config) {
        boolean valid = true;
        breadcrumbs = breadcrumbs + ".config";
        if (getSeparator(dc, config) == null) {
            validationReport.get("error").add(breadcrumbs + ".separator: missing required field: file separator must be specified here or in defaultConfig");
            valid = false;
        }
        if (getRowsPerCommit(dc, config) == null) {
            validationReport.get("error").add(breadcrumbs + ".rowsPerCommit: missing required field: rowsPerCommit must be specified here or in defaultConfig");
            valid = false;
        }
        return valid;
    }

    public void validateSchemaPresent(HashMap<String, ArrayList<String>> validationReport) {
        String schemaPath = configuration.getGlobalConfig().getSchemaPath();
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

    private boolean valDataPaths(HashMap<String, ArrayList<String>> validationReport,
                                 String breadcrumbs,
                                 Configuration.Generator generator) {
        boolean valid = true;

        String[] dataPaths = generator.getDataPaths();

        // dataPaths missing
        if (dataPaths == null) {
            validationReport.get("errors").add(breadcrumbs + ".dataPaths: missing required field");
            valid = false;
        }
        // dataPaths empty
        if (dataPaths != null && dataPaths.length < 1) {
            validationReport.get("errors").add(breadcrumbs + ".dataPaths: provided empty list - must have at least one file");
            valid = false;
        }

        // file missing or empty
        Character fileSeparator = getSeparator(configuration, generator.getConfig());
        if (dataPaths != null && fileSeparator != null) {
            for (String filepath : dataPaths) {
                try {
                    Util.getFileHeader(filepath, fileSeparator);
                } catch (IOException fileNotFoundException) {
                    validationReport.get("errors").add(breadcrumbs + ".dataPaths: <" + filepath + ">: file not found");
                    valid = false;
                } catch (IllegalArgumentException ioException) {
                    validationReport.get("errors").add(breadcrumbs + ".dataPaths: <" + filepath + ">: file is empty");
                    valid = false;
                }
            }
        }
        return valid;
    }

    private void valColumnInHeader(HashMap<String, ArrayList<String>> validationReport,
                                   String breadcrumbs,
                                   Configuration configuration,
                                   Configuration.Generator generator,
                                   String column) {
        for (String dataPath : generator.getDataPaths()) {
            try {
                String[] header = Util.getFileHeader(dataPath, getSeparator(configuration, generator.getConfig()));
                if (Arrays.stream(header).noneMatch(headerColumn -> headerColumn.equals(column))) {
                    validationReport.get("errors").add(breadcrumbs + ".column: <" + column + "> column not found in header of file <" + dataPath + ">");
                }
            } catch (IOException | IllegalArgumentException ignored) {
            }
        }
    }

    private boolean valConceptTypeInSchema(HashMap<String, ArrayList<String>> validationReport,
                                           TypeDBSession session,
                                           String breadcrumbs,
                                           String conceptType) {
        boolean valid = true;
        TypeQLMatch query = TypeQL.match(TypeQL.var("t").type(conceptType));
        try (TypeDBTransaction txn = session.transaction(TypeDBTransaction.Type.READ)) {
            Util.trace(Integer.toString((int) txn.query().match(query).count()));
        } catch (TypeDBClientException typeDBClientException) {
            if (typeDBClientException.toString().contains("Invalid Type Read: The type '" + conceptType + "' does not exist.")) {
                validationReport.get("errors").add(breadcrumbs + ".conceptType: <" + conceptType + "> does not exist in schema");
                valid = false;
            } else {
                throw typeDBClientException;
            }
        }
        return valid;
    }

    private void valEntityHasAttributes(HashMap<String, ArrayList<String>> validationReport,
                                        String breadcrumbs,
                                        Configuration configuration,
                                        Configuration.Generator generator,
                                        Configuration.ConstrainingAttribute[] constrainingAttributes,
                                        TypeDBSession session) {
        if (constrainingAttributes == null) {
            validationReport.get("errors").add(breadcrumbs + ".attributes: missing required attributes list");
        } else {
            valHasAttributes(validationReport, breadcrumbs, configuration, generator, constrainingAttributes, session);
        }
    }

    private void valRelationHasAttributes(HashMap<String, ArrayList<String>> validationReport,
                                          String breadcrumbs,
                                          Configuration configuration,
                                          Configuration.Generator generator,
                                          Configuration.ConstrainingAttribute[] constrainingAttributes,
                                          TypeDBSession session) {
        if (constrainingAttributes != null) {
            valHasAttributes(validationReport, breadcrumbs, configuration, generator, constrainingAttributes, session);
        }
    }

    private void valHasAttributes(HashMap<String, ArrayList<String>> validationReport,
                                  String breadcrumbs,
                                  Configuration configuration,
                                  Configuration.Generator generator,
                                  Configuration.ConstrainingAttribute[] constrainingAttributes,
                                  TypeDBSession session
    ) {
        int entryIdx = 0;
        for (Configuration.ConstrainingAttribute attribute : constrainingAttributes) {
            String aBreadcrumbs = breadcrumbs + ".attributes.[" + entryIdx + "]";
            valConstrainingAttribute(validationReport, aBreadcrumbs, configuration, generator, attribute, session);
            entryIdx += 1;
        }
    }

    private void valRelationPlayers(HashMap<String, ArrayList<String>> validationReport,
                                    String breadcrumbs,
                                    Configuration configuration,
                                    Configuration.Relation relation,
                                    TypeDBSession session) {
        int playerIdx = 0;
        for (Configuration.Player player : relation.getPlayers()) {


            // Role Type exists
            if (player.getRoleType() == null) {
                validationReport.get("errors").add(breadcrumbs + ".players.[" + playerIdx + "]" + ".roleType: missing required field");
            } else {
                valRoleType(validationReport, session, breadcrumbs + ".players.[" + playerIdx + "]", relation.getConceptType(), player.getRoleType());
            }

            String pBreadcrumbs = breadcrumbs + ".players." + player.getRoleType();
            // requireNonEmpty
            if (player.getRequireNonEmpty() == null) {
                validationReport.get("warnings").add(pBreadcrumbs + ".requireNonEmpty: field not set - defaults to false");
            }

            // Role Getter
            if (player.getRoleGetter() != null) {
                String gBreadcrumbs = pBreadcrumbs + ".roleGetter";

                Configuration.RoleGetter roleGetter = player.getRoleGetter();
                // CONCEPT_TYPE
                if (roleGetter.getConceptType() == null) {
                    validationReport.get("errors").add(gBreadcrumbs + ".conceptType: missing required field");
                } else {
                    valConceptTypeInSchema(validationReport, session, gBreadcrumbs, roleGetter.getConceptType());
                }

                // HANDLER
                if (roleGetter.getHandler() == null) {
                    validationReport.get("errors").add(gBreadcrumbs + ".handler: missing required field");
                } else {
                    if (roleGetter.getHandler() != TypeHandler.ENTITY &&
                            roleGetter.getHandler() != TypeHandler.RELATION &&
                            roleGetter.getHandler() != TypeHandler.ATTRIBUTE) {
                        validationReport.get("errors").add(gBreadcrumbs + ".handler: must be either \"attribute\", \"entity\", or \"relation\", but is: " + roleGetter.getHandler());
                    } else {
                        if (roleGetter.getHandler().equals(TypeHandler.ATTRIBUTE)) {
                            // thingGetters must not be set
                            if (roleGetter.getThingGetters() != null) {
                                validationReport.get("error").add(gBreadcrumbs + ".thingGetters: .handler=\"attribute\" is directly gotten by setting .column");
                            }
                            // COLUMN
                            if (roleGetter.getColumn() == null) {
                                validationReport.get("error").add(breadcrumbs + ".column: missing required field");
                            } else {
                                valColumnInHeader(validationReport, breadcrumbs, configuration, relation, roleGetter.getColumn());
                            }
                        } else if (roleGetter.getHandler().equals(TypeHandler.ENTITY)) {
                            valThingGettersEntity(validationReport, breadcrumbs, configuration, relation, roleGetter.getThingGetters(), session);
                        } else if (roleGetter.getHandler().equals(TypeHandler.RELATION)) {
                            valThingGettersRelation(validationReport, breadcrumbs, configuration, relation, roleGetter.getThingGetters(), session);
                        }
                    }
                }
            } else {
                validationReport.get("errors").add(pBreadcrumbs + ".roleGetter: missing required roleGetter object");
            }

            playerIdx += 1;
        }
    }

    public void valThingGettersEntity(HashMap<String, ArrayList<String>> validationReport,
                                      String breadcrumbs,
                                      Configuration configuration,
                                      Configuration.Relation relation,
                                      Configuration.ThingGetter[] thingGetters,
                                      TypeDBSession session) {

        int tgi = 0;
        if (thingGetters == null) {
            validationReport.get("errors").add(breadcrumbs + ".thingGetters: missing required object");
        } else {
            if (thingGetters.length < 1) {
                validationReport.get("errors").add(breadcrumbs + ".thingGetters: must contain at least one ownership attribute");
            } else {
                for (Configuration.ThingGetter thingGetter : thingGetters) {
                    String tgBreadcrumbs = breadcrumbs + ".thingGetters.[" + tgi + "]";
                    // HANDLER
                    if (thingGetter.getHandler() == null) {
                        validationReport.get("errors").add(tgBreadcrumbs + ".handler: missing required field");
                    } else {
                        if (!thingGetter.getHandler().equals(TypeHandler.OWNERSHIP)) {
                            validationReport.get("errors").add(tgBreadcrumbs + ".handler: when getting entities, .handler must be of type \"ownership\"");
                        }
                    }
                    // CONCEPT_TYPE
                    if (thingGetter.getConceptType() == null) {
                        validationReport.get("errors").add(tgBreadcrumbs + ".conceptType: missing required field");
                    } else {
                        valConceptTypeInSchema(validationReport, session, tgBreadcrumbs, thingGetter.getConceptType());
                    }
                    // COLUMN
                    if (thingGetter.getColumn() == null) {
                        validationReport.get("errors").add(tgBreadcrumbs + ".column: missing required field");
                    } else {
                        valColumnInHeader(validationReport, tgBreadcrumbs, configuration, relation, thingGetter.getColumn());
                    }
                    tgi = tgi + 1;
                }
            }
        }
    }

    public void valThingGettersRelation(HashMap<String, ArrayList<String>> validationReport,
                                        String breadcrumbs,
                                        Configuration configuration,
                                        Configuration.Relation relation,
                                        Configuration.ThingGetter[] thingGetters,
                                        TypeDBSession session) {
        if (thingGetters == null) {
            validationReport.get("errors").add(breadcrumbs + ".thingGetters: missing required object");
        } else {
            if (thingGetters.length < 1) {
                validationReport.get("errors").add(breadcrumbs + ".thingGetters: must contain at least one ownership or entity entry");
            } else {
                //TODO by attribute

                //TODO by players
            }
        }
    }

    private void valRoleType(HashMap<String, ArrayList<String>> validationReport,
                             TypeDBSession session,
                             String breadcrumbs,
                             String relationConceptType,
                             String roleType) {
        try (TypeDBTransaction txn = session.transaction(TypeDBTransaction.Type.READ)) {
            TypeQLMatch query = TypeQL.match(TypeQL.type(relationConceptType).relates(TypeQL.var("r"))).get("r");
            Stream<ConceptMap> answers = txn.query().match(query);

            if (answers.noneMatch(a -> a.get("r").asRoleType().getLabel().name().equals(roleType))) {
                validationReport.get("errors").add(breadcrumbs + ".roleType: <" + roleType + "> is not a role for relation of type <" + relationConceptType + "> in schema");
            }
        }
    }

}
