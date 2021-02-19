package util;

import java.io.*;
import java.util.ArrayList;

public class Util {
    public static String getAbsPath(String p) {
        File file = new File(p);
        return file.getAbsolutePath();
    }

    public static int getListIndexOfShortestString(ArrayList<String> batchStrings) {
        int shortest = 0;
        for (String s : batchStrings) {
            if (s.length() < batchStrings.get(shortest).length()) {
                shortest = batchStrings.indexOf(s);
            }
        }
        return shortest;
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
}
