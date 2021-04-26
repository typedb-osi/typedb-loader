package processor;

import configuration.ConfigEntryData;
import configuration.ConfigEntryProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NestedRelationInsertProcessor extends RelationInsertProcessor {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");

    public NestedRelationInsertProcessor(ConfigEntryData dce, ConfigEntryProcessor configEntryProcessor, int dataPathIndex) {
        super(dce, configEntryProcessor, dataPathIndex);
        appLogger.debug("Creating NestedRelationInsertGenerator for " + pce.getProcessor() + " of type " + pce.getProcessorType());
    }

}