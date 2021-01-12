package generator;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RelationOfRelationInsertGenerator extends RelationInsertGenerator {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");

    public RelationOfRelationInsertGenerator(DataConfigEntry dce, ProcessorConfigEntry processorConfigEntry) {
        super(dce, processorConfigEntry);
        appLogger.debug("Creating RelationOfRelationInsertGenerator for " + pce.getProcessor() + " of type " + pce.getProcessorType());
    }

}