package generator;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NestedRelationInsertGenerator extends RelationInsertGenerator {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");

    public NestedRelationInsertGenerator(DataConfigEntry dce, ProcessorConfigEntry processorConfigEntry) {
        super(dce, processorConfigEntry);
        appLogger.debug("Creating NestedRelationInsertGenerator for " + pce.getProcessor() + " of type " + pce.getProcessorType());
    }

}