package configuration;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;

public class MigrationConfig {

    private final String dataConfigPath;
    private final String processorConfigPath;
    private final String schemaPath;
    private final String keyspace;
    private final String graknURI;
    private HashMap<String, DataConfigEntry> dataConfig;
    private HashMap<String, ArrayList<ProcessorConfigEntry>> processorConfig;

    public MigrationConfig(String graknURI, String keyspace, String schemaPath, String dataConfigPath, String processorConfigPath) {
        this.graknURI = graknURI;
        this.keyspace = keyspace;
        this.schemaPath = schemaPath;
        this.dataConfigPath = dataConfigPath;
        this.processorConfigPath = processorConfigPath;
        initializeDataConfig();
        initializeProcessorConfig();
    }

    private void initializeDataConfig() {
        BufferedReader bufferedReader;
        try {
            bufferedReader = new BufferedReader(new FileReader(dataConfigPath));
            Type ConfigType = new TypeToken<HashMap<String, DataConfigEntry>>() {}.getType();
            this.dataConfig = new Gson().fromJson(bufferedReader, ConfigType);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public HashMap<String, DataConfigEntry> getDataConfig() {
        return this.dataConfig;
    }

    public void initializeProcessorConfig() {
        BufferedReader bufferedReader;
        try {
            bufferedReader = new BufferedReader(new FileReader(processorConfigPath));
            Type ConfigType = new TypeToken<HashMap<String, ArrayList<ProcessorConfigEntry>>>() {
            }.getType();
            this.processorConfig = new Gson().fromJson(bufferedReader, ConfigType);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public HashMap<String, ArrayList<ProcessorConfigEntry>> getProcessorConfig() {
        return this.processorConfig;
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
