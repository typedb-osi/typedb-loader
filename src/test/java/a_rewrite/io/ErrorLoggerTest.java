package a_rewrite.io;

import org.junit.Test;

public class ErrorLoggerTest {

    @Test
    public void errorFileLogger() {
        ErrorFileLogger logger = ErrorFileLogger.getLogger();
        logger.logMalformed("entities.tsv", "shucks! There was a malformed row error");
        logger.logUnavailable("entities.tsv", "shucks! There was a connection error");
        logger.logInvalid("entities.tsv", "shucks! There was a invalid row error");
    }

}
