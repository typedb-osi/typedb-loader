package configuration;

public class LoaderSchemaUpdateConfig {

    private final String schemaPath;
    private final String keyspace;
    private final String graknURI;

    public LoaderSchemaUpdateConfig(String graknURI, String keyspace, String schemaPath) {
        this.graknURI = graknURI;
        this.keyspace = keyspace;
        this.schemaPath = schemaPath;
    }

    public String getSchemaPath() {
        return this.schemaPath;
    }

    public String getKeyspace() {
        return this.keyspace;
    }

    public String getGraknURI() {
        return graknURI;
    }
}
