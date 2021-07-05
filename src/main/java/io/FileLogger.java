package io;

import org.apache.commons.io.FilenameUtils;

import java.io.FileWriter;
import java.io.IOException;

public class FileLogger {

    private static FileLogger logger = null;

    private FileLogger() {
    }

    public synchronized void logToErrorSummary(String errorString) {
        try {
            FileWriter fw = new FileWriter("typedb-loader-data.log", true);
            fw.append(errorString.replace("null", ""));
            fw.append("\n");
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public synchronized void logToLoadingSummary(String errorString) {
        try {
            FileWriter fw = new FileWriter("typedb-loader-loading.log", true);
            fw.append(errorString.replace("null", ""));
            fw.append("\n");
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public synchronized void logMalformed(String sourceFile, String errorString)
    {
        try {
            FileWriter fw = new FileWriter(FilenameUtils.removeExtension(sourceFile) + "_malformed.log", true);
            fw.append(errorString.replace("null", ""));
            fw.append("\n");
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public synchronized void logInvalid(String sourceFile, String errorString)
    {
        try {
            FileWriter fw = new FileWriter(FilenameUtils.removeExtension(sourceFile) + "_invalid.log", true);
            fw.append(errorString.replace("null", ""));
            fw.append("\n");
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public synchronized void logUnavailable(String sourceFile, String errorString)
    {
        try {
            FileWriter fw = new FileWriter(FilenameUtils.removeExtension(sourceFile) + "_unavailable.log", true);
            fw.append(errorString.replace("null", ""));
            fw.append("\n");
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public synchronized void logColumnWarnings(String sourceFile, String errorString)
    {
        try {
            FileWriter fw = new FileWriter(FilenameUtils.removeExtension(sourceFile) + "_column_type.log", true);
            fw.append(errorString.replace("null", ""));
            fw.append("\n");
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public static synchronized FileLogger getLogger() {
        if (logger == null) {
            logger = new FileLogger();
        }
        return logger;
    }
}
