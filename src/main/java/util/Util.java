package util;

import config.Configuration;
import config.TypeHandler;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import io.FileLogger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public class Util {
    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.tdl.loader");
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

    public static Configuration initializeDataConfig(String dcPath) {
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
        String schema="";
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(schemaPath)));
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
        return new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
    }

    public static String[] parseBySeparator(String line, char separator) throws IOException {
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

    public static void setConstrainingAttributeConceptType(Configuration.ConstrainingAttribute[] attributes, TypeDBSession session) {
        for (Configuration.ConstrainingAttribute attribute : attributes) {
            attribute.setConceptValueType(session.transaction(TypeDBTransaction.Type.READ));
        }
    }

    public static void setGetterAttributeConceptType(Configuration.Relation relation, int playerIndex, TypeDBSession session) {
        if (relation.getPlayers()[playerIndex].getRoleGetter() != null) {
            Configuration.ThingGetter[] ownershipThingGetters = relation.getPlayers()[playerIndex].getRoleGetter().getOwnershipThingGetters();
            if (ownershipThingGetters != null) {
                for (Configuration.ThingGetter ownershipRoleGetter : ownershipThingGetters) {
                    ownershipRoleGetter.setConceptValueType(session.transaction(TypeDBTransaction.Type.READ));
                }
            }
            Configuration.ThingGetter[] thingGetters = relation.getPlayers()[playerIndex].getRoleGetter().getThingGetters();
            if (thingGetters != null) {
                for (Configuration.ThingGetter thingGetter : thingGetters) {
                    if (thingGetter != null) {
                        Configuration.ThingGetter[] thingThingGetters = thingGetter.getThingGetters();
                        if (thingThingGetters != null) {
                            for (Configuration.ThingGetter thingThingGetter : thingThingGetters) {
                                if (thingThingGetter.getHandler() == TypeHandler.OWNERSHIP) {
                                    thingThingGetter.setConceptValueType(session.transaction(TypeDBTransaction.Type.READ));
                                }
                            }
                        }
                    }
                }
            }
        }
        Configuration.RoleGetter attributeRoleGetter = relation.getPlayers()[playerIndex].getRoleGetter();
        if (attributeRoleGetter != null && attributeRoleGetter.getHandler() == TypeHandler.ATTRIBUTE) {
            attributeRoleGetter.setConceptValueType(session.transaction(TypeDBTransaction.Type.READ));
        }
    }

    public static int getRowsPerCommit(Configuration dc, Configuration.GeneratorConfig config) {
        if (config != null) {
            return Objects.requireNonNullElseGet(config.getRowsPerCommit(), () -> dc.getDefaultConfig().getRowsPerCommit());
        } else {
            return dc.getDefaultConfig().getRowsPerCommit();
        }
    }

    public static Character getSeparator(Configuration dc, Configuration.GeneratorConfig config) {
        if (config != null) {
            return Objects.requireNonNullElseGet(config.getSeparator(), () -> dc.getDefaultConfig().getSeparator());
        } else {
            return dc.getDefaultConfig().getSeparator();
        }
    }
}
