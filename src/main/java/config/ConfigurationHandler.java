package config;

public enum ConfigurationHandler {
    ATTRIBUTES,
    ENTITIES,
    RELATIONS;

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
