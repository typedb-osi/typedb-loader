/*
 * Copyright (C) 2021 Bayer AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.vaticle.typedb.osi.loader.io;

import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class FileLogger {

    private static FileLogger logger = null;
    private final String directoryString;

    private FileLogger() {
        DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH_mm_ss").withZone(ZoneId.systemDefault());
        File directory = new File(DATE_TIME_FORMATTER.format(new Date().toInstant()));
        this.directoryString = directory.toString();
        if (! directory.exists()){
            directory.mkdir();
        }
    }

    public static synchronized FileLogger getLogger() {
        if (logger == null) {
            logger = new FileLogger();
        }
        return logger;
    }

    public synchronized void logMalformed(String sourceFile, String errorString) {
        try {
            FileWriter fw = new FileWriter(directoryString + "/" + FilenameUtils.removeExtension(sourceFile) + "_malformed.log", true);
            fw.append(errorString.replace("null", ""));
            fw.append("\n");
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public synchronized void logInvalid(String sourceFile, String errorString) {
        try {
            FileWriter fw = new FileWriter(directoryString + "/" + FilenameUtils.removeExtension(sourceFile) + "_invalid.log", true);
            fw.append(errorString.replace("null", ""));
            fw.append("\n");
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public synchronized void logUnavailable(String sourceFile, String errorString) {
        try {
            FileWriter fw = new FileWriter(directoryString + "/" + FilenameUtils.removeExtension(sourceFile) + "_unavailable.log", true);
            fw.append(errorString.replace("null", ""));
            fw.append("\n");
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public void logNoMatches(String sourceFile, String row) {
        try {
            FileWriter fw = new FileWriter(directoryString + "/" + FilenameUtils.removeExtension(sourceFile) + "_no_matches.log", true);
            fw.append(row.replace("null", ""));
            fw.append("\n");
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public void logTooManyMatches(String sourceFile, String row) {
        try {
            FileWriter fw = new FileWriter(directoryString + "/" + FilenameUtils.removeExtension(sourceFile) + "_too_many_matches.log", true);
            fw.append(row.replace("null", ""));
            fw.append("\n");
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public synchronized void logColumnWarnings(String sourceFile, String errorString) {
        try {
            FileWriter fw = new FileWriter(directoryString + "/" + FilenameUtils.removeExtension(sourceFile) + "_column_type.log", true);
            fw.append(errorString.replace("null", ""));
            fw.append("\n");
            fw.flush();
            fw.close();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }
}
