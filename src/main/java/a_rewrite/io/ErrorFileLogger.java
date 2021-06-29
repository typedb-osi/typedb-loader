package a_rewrite.io;

import org.apache.commons.io.FilenameUtils;

import java.io.FileWriter;
import java.io.IOException;

public class ErrorFileLogger {

    private static ErrorFileLogger logger = null;

    private ErrorFileLogger() {
    }

    public synchronized void logMalformed(String sourceFile, String errorString)
    {
        try {
            FileWriter fw = new FileWriter(FilenameUtils.removeExtension(sourceFile) + "_malformed.log");
            fw.append(errorString);
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public synchronized void logInvalid(String sourceFile, String errorString)
    {
        try {
            FileWriter fw = new FileWriter(FilenameUtils.removeExtension(sourceFile) + "_invalid.log");
            fw.append(errorString);
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public synchronized void logUnavailable(String sourceFile, String errorString)
    {
        try {
            FileWriter fw = new FileWriter(FilenameUtils.removeExtension(sourceFile) + "_unavailable.log");
            fw.append(errorString);
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public synchronized void logColumnWarnings(String sourceFile, String errorString)
    {
        try {
            FileWriter fw = new FileWriter(FilenameUtils.removeExtension(sourceFile) + "_column_type.log");
            fw.append(errorString);
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public static synchronized ErrorFileLogger getLogger() {
        if (logger == null) {
            logger = new ErrorFileLogger();
        }
        return logger;
    }
}
