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

package com.vaticle.typedb.osi.loader.util;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.vaticle.typedb.client.api.TypeDBSession;
import com.vaticle.typedb.osi.loader.config.Configuration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import static com.vaticle.typedb.osi.loader.io.FileToInputStream.getInputStream;
import static java.nio.charset.StandardCharsets.UTF_8;

public class Util {

    private static final Logger appLogger = LogManager.getLogger("com.vaticle.typedb.osi.loader.error");
    //TODO: disallow duplicate header names, and why does ignoreEmptyLines not work???
    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withEscape('\\').withIgnoreSurroundingSpaces().withNullString("");
    private static final CSVFormat TSV_FORMAT = CSVFormat.DEFAULT.withDelimiter('\t').withEscape('\\').withIgnoreSurroundingSpaces().withNullString("");

    public static String[] getFileHeader(String filePath, char separator) throws IOException, IllegalArgumentException {
        BufferedReader br = newBufferedReader(filePath);
        if (separator == ',') {
            return parseCSV(br.readLine());
        } else if (separator == '\t') {
            return parseTSV(br.readLine());
        } else {
            throw new IllegalArgumentException("currently supported separators are: <,>, <\t>");
        }
    }

    public static String getAbsPath(String p) {
        File file = new File(p);
        return file.getAbsolutePath();
    }

    public static Configuration initializeConfig(String dcPath) {
        try {
            BufferedReader bufferedReader = newBufferedReader(dcPath);
            Type DataConfigType = new TypeToken<Configuration>() {
            }.getType();
            return new Gson().fromJson(bufferedReader, DataConfigType);
        } catch (FileNotFoundException e) {
            appLogger.error("Data Configuration file not found - please check the path: " + dcPath);
            return null;
        }
    }

    public static String loadSchemaFromFile(String schemaPath) {
        String schema = "";
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(schemaPath), UTF_8));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line).append("\n");
                line = br.readLine();
            }
            schema = sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return schema;
    }

    public static BufferedReader newBufferedReader(String filePath) throws FileNotFoundException {
        InputStream is = getInputStream(filePath);
        if (is != null) {
            return new BufferedReader(new InputStreamReader(is, UTF_8));
        } else {
            throw new FileNotFoundException();
        }
    }

    public static String[] parseBySeparator(String line, char separator) throws IllegalArgumentException {
        if (separator == ',') {
            return parseCSV(line);
        } else if (separator == '\t') {
            return parseTSV(line);
        } else {
            throw new IllegalArgumentException("currently supported separators are: <,>, <\t>");
        }
    }

    public static String[] parseCSV(String line) {
        if (!line.isEmpty()) {
            try {
                CSVRecord csv = CSVParser.parse(line, CSV_FORMAT).getRecords().get(0);
                return parse(csv);
            } catch (IOException ioException) {
                line = line.replace("\"", "");
                try {
                    CSVRecord csv = CSVParser.parse(line, CSV_FORMAT).getRecords().get(0);
                    return parse(csv);
                } catch (IOException ioException2) {
                    //TODO LOG INTO ERRORS
                    return new String[0];
                }
            }
        } else {
            return new String[0];
        }
    }

    public static String[] parseTSV(String line) {
        if (!line.isEmpty()) {
            try {
                CSVRecord tsv = CSVParser.parse(line, TSV_FORMAT).getRecords().get(0);
                return parse(tsv);
            } catch (IOException ioException) {
                line = line.replace("\"", "");
                try {
                    CSVRecord tsv = CSVParser.parse(line, TSV_FORMAT).getRecords().get(0);
                    return parse(tsv);
                } catch (IOException ioException2) {
                    //TODO LOG INTO ERRORS
                    return new String[0];
                }
            }
        } else {
            return new String[0];
        }
    }

    private static String[] parse(CSVRecord record) {
        String[] arr = new String[record.size()];
        for (int i = 0; i < record.size(); i++) {
            arr[i] = record.get(i);
            if (arr[i] != null) {
                if ((arr[i].equals("\\N") || arr[i].equalsIgnoreCase("null"))) arr[i] = null;
                else arr[i] = escapeDoubleQuotes(arr[i]);
            }
        }
        return arr;
    }

    public static String escapeDoubleQuotes(String toFormat) {
        return toFormat.replaceAll("\"", "\\\\\"");
    }

    public static void debug(String message,
                             Object... objects) {
        if (appLogger.isDebugEnabled())
            appLogger.debug(message, objects);
    }

    public static void error(String message,
                             Object... objects) {
        appLogger.error(message, objects);
    }

    public static void warn(String message,
                            Object... objects) {
        appLogger.warn(message, objects);

    }

    public static void info(String message,
                            Object... objects) {
        appLogger.info(message, objects);
    }

    public static void trace(String message,
                             Object... objects) {
        appLogger.trace(message, objects);
    }

    public static double calculateRate(double count,
                                       Instant start,
                                       Instant end) {
        return count * Duration.ofSeconds(1).toMillis() / Duration.between(start, end).toMillis();
    }

    public static String printDuration(Instant start,
                                       Instant end) {
        return Duration.between(start, end).toString()
                .substring(2)
                .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                .toLowerCase();
    }

    public static void setConstrainingAttributeConceptType(Configuration.Definition.Attribute[] attributes, TypeDBSession session) {
        for (Configuration.Definition.Attribute attribute : attributes) {
            attribute.setConceptValueType(session);
        }
    }

    public static String playerType(Configuration.Definition.Player player) {
        if (player.getMatch().getOwnerships() != null) {
            return "byAttribute";
        } else if (player.getMatch().getAttribute() != null) {
            return "attribute";
        } else if (player.getMatch().getPlayers() != null) {
            return "byPlayer";
        } else {
            return "";
        }
    }

    public static Integer getRowsPerCommit(Configuration dc, Configuration.Generator.GeneratorConfig config) {
        if (config != null) {
            return Objects.requireNonNullElseGet(config.getRowsPerCommit(), () -> dc.getGlobalConfig().getRowsPerCommit());
        } else {
            return dc.getGlobalConfig().getRowsPerCommit();
        }
    }

    public static Character getSeparator(Configuration dc, Configuration.Generator.GeneratorConfig config) {
        if (config != null) {
            return Objects.requireNonNullElseGet(config.getSeparator(), () -> dc.getGlobalConfig().getSeparator());
        } else {
            return dc.getGlobalConfig().getSeparator();
        }
    }
}
