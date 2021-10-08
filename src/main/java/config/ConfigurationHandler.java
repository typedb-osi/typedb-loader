package config;

public enum ConfigurationHandler {
    ATTRIBUTES,
    ENTITIES,
    RELATIONS,
    APPEND_ATTRIBUTE,
    APPEND_ATTRIBUTE_OR_INSERT_THING;

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
