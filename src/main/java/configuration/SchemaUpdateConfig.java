package configuration;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;

public class SchemaUpdateConfig {

    private final String schemaPath;
    private final String keyspace;
    private final String graknURI;

    public SchemaUpdateConfig(String graknURI, String keyspace, String schemaPath) {
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
