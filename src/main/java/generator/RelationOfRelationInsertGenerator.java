package generator;

import configuration.DataConfigEntry;
import configuration.ProcessorConfigEntry;
import graql.lang.Graql;
import graql.lang.statement.Statement;
import graql.lang.statement.StatementInstance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static generator.GeneratorUtil.*;

// TODO: match relation by player combination

public class RelationOfRelationInsertGenerator extends RelationInsertGenerator {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");
    private static final Logger dataLogger = LogManager.getLogger("com.bayer.dt.grami.data");

    public RelationOfRelationInsertGenerator(DataConfigEntry dce, ProcessorConfigEntry processorConfigEntry) {
        super(dce, processorConfigEntry);
        appLogger.debug("Creating RelationOfRelationInsertGenerator for " + gce.getProcessor() + " of type " + gce.getProcessorType());
    }

}