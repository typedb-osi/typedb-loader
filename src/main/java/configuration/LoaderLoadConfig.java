package configuration;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;

public class LoaderLoadConfig {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private final String dataConfigPath;
    private final String processorConfigPath;
    private final String schemaPath;
    private final String keyspace;
    private final String graknURI;
    private HashMap<String, ConfigEntryData> dataConfig;
    private HashMap<String, ArrayList<ConfigEntryProcessor>> processorConfig;

    public LoaderLoadConfig(String graknURI, String keyspace, String schemaPath, String dataConfigPath, String processorConfigPath) {
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
            Type ConfigType = new TypeToken<HashMap<String, ConfigEntryData>>() {
            }.getType();
            this.dataConfig = new Gson().fromJson(bufferedReader, ConfigType);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public HashMap<String, ConfigEntryData> getDataConfig() {
        return this.dataConfig;
    }

    public void initializeProcessorConfig() {
        BufferedReader bufferedReader;
        try {
            bufferedReader = new BufferedReader(new FileReader(processorConfigPath));
            Type ConfigType = new TypeToken<HashMap<String, ArrayList<ConfigEntryProcessor>>>() {
            }.getType();
            try {
                this.processorConfig = new Gson().fromJson(bufferedReader, ConfigType);
                resolveProcessorReference();
            } catch (JsonSyntaxException ex) {
                appLogger.error("you have a duplicate key somewhere in your processor configuration file - must be fixed, cannot migrate");
                System.exit(1);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void resolveProcessorReference() {
        for (ConfigEntryProcessor pce : processorConfig.get("processors")) {
            if (pce.getReferenceProcessor() != null) {
                ConfigEntryProcessor referencedPCE = getReferenceProcessorConfigEntry(pce.getReferenceProcessor());
                if (referencedPCE != null) {
                    pce.setSchemaType(referencedPCE.getSchemaType());
                    pce.setConceptProcessors(referencedPCE.getConceptProcessors());
                } else {
                    appLogger.error("the referenced processor <" + pce.getReferenceProcessor() + "> does not exist in the processor configuration file.");
                }
            }
        }
    }

    private ConfigEntryProcessor getReferenceProcessorConfigEntry(String referenceProcessor) {
        for (ConfigEntryProcessor pce : processorConfig.get("processors")) {
            if (pce.getProcessor().equals(referenceProcessor)) {
                return pce;
            }
        }
        return null;
    }

    public HashMap<String, ArrayList<ConfigEntryProcessor>> getProcessorConfig() {
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