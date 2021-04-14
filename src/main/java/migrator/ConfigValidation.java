package migrator;

import configuration.MigrationConfig;
import configuration.ProcessorConfigEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ConfigValidation {

    public static ArrayList<ValidationReport> validateConfigs(MigrationConfig migrationConfig) {
        ArrayList<ValidationReport> reports = new ArrayList<>();
        reports.add(validateProcessorConfigAgainstSchema(migrationConfig));
        reports.add(validateDataConfigAgainstProcessorConfig(migrationConfig));
        return reports;
    }

    private static ValidationReport validateProcessorConfigAgainstSchema(MigrationConfig migrationConfig) {
        HashMap<String, ArrayList<ProcessorConfigEntry>> procConfig = migrationConfig.getProcessorConfig();

        ArrayList<String> errors = new ArrayList<>();

        for (Map.Entry<String, ArrayList<ProcessorConfigEntry>> entry: procConfig.entrySet()) {
            ArrayList<ProcessorConfigEntry> processors = entry.getValue();
            int processorIndex = 0;
            for (ProcessorConfigEntry processor : processors) {
                String processorName = processor.getProcessor();
                if(processorName.isEmpty()) {
                    errors.add("ProcessorConfigEntry at index " + processorIndex + " has an empty \"processor\" field");
                }
                String res = validateProcessorType(processor.getProcessorType());
                if (!res.isEmpty()) errors.add("In Processor <" + processorName + ">: " + res);

                processorIndex = processorIndex + 1;
            }
        }

        // for each entry:
        // "processorType" must be one of enum of entity, relation, nested-relation, etc...
        // "schemaType" must exist
        // "processor" must be unique and non-empty
        // if entity
        // conceptGenerators and attributes must be present
        // foreach attribute
        // key must be unique and non-empty
        // "attributeType" must specify existing attribute of "schemaType" & have correct "valueType"
        // "required" must be present as either false/true (boolean)
        // if relation
        // conceptGenerators and players must be present, attributes optionally
        // for each attribute
        // key must be unique and non-empty
        // "attributeType" must specify existing attribute of "schemaType" & have correct "valueType"
        // "required" must be present as either false/true (boolean)
        // for each player
        // key must be unique and non-empty
        // "playerType" must refer to existing "schemaType" and play "roleType" and be an entity
        // "roleType" must be a role in "schemaType"
        // "uniquePlayerId" must be existing attribute for "playerType" & "idValueType" must be of correct value type
        // "required" must be present as either false/true (boolean)
        // if nested-relation
        // conceptGenerators and relationPlayers must be present, attributes and players optionally present
        // for each attribute
        // key must be unique and non-empty
        // "attributeType" must specify existing attribute of "schemaType" & have correct "valueType"
        // "required" must be present as either false/true (boolean)
        // for each player
        // key must be unique and non-empty
        // "playerType" must refer to existing "schemaType" and play "roleType" and be an entity
        // "roleType" must be a role in "schemaType"
        // "uniquePlayerId" must be existing attribute for "playerType" & "idValueType" must be of correct value type
        // "required" must be present as either false/true (boolean)
        // for each "relationPlayers"
        // key must be unique and non-empty
        // "playerType" must exist as relation in schema and must play "roleType" in "SchemaType" relation
        // required must be present as either false/true
        // must have either "matchByAttribute" or "matchByPlayer", or both
        // foreach "matchByAttribute"
        // key must be unique and non-empty
        // "attributeType" must exist as attribute for "playerType" and "valueType" must be correct
        // foreach matchByPlayer
        // key must be unique and non-empty
        // "playerType" must exist and be an entity
        // "uniquePlayerId" must be attribute of "playerType" and have correct "idValueType"
        // "playerType" must play "roleType" and "roleType" must be role of parent-"playerType" relation
        // required must be present as either false/true
        // if append-attribute
        // conceptGenerators and attributes must be present
        // foreach attribute
        // key must be present as either true/false (boolean)
        // "attributeType" must be an attribute for "schemaType" and be of correct "valueType"
        // required must be present as either true/false --> only for those that are not special case in dataconfig!!!
        // if attribute
        // conceptGenerators and attributes must be present
        // foreach attribute
        // only a single key allowed must be unique and non-empty
        // "attributeType" must be an attribute in schema with correct "valueType"
        // all other fields are disallowed
        // if attribute relation
        // conceptGenerators and players must be present, attributes optional
        // foreach attribute as above
        // foreach player
        // key must be unique and non-empty
        // "playerType" must be an attribute or an entity
        // if attribute:
        // "uniquePlayerId" must be "_attribute_player_"
        // "idValueType must be the correct value type
        // "roleType" must be played by the attribute and the relation must have it as a player
        // required must be present as boolean true/false
        // if entity
        // do above validation
        return new ValidationReport("proc-config", errors.size() <= 0, errors);
    }

    private static String validateProcessorType(String processorType) {
        boolean flag = false;
        for (ProcessorTypes proctype: ProcessorTypes.values()) {
            if (proctype.toString().equals(processorType)) {
                flag = true;
                break;
            }
        }
        if (flag) {
            return "";
        }
        return processorType + " is not a valid processorType";
    }

    private static ValidationReport validateDataConfigAgainstProcessorConfig(MigrationConfig migrationConfig) {
        ArrayList<String> errors = new ArrayList<>();
        return new ValidationReport("data-config", errors.size() <= 0, errors);
    }

}
