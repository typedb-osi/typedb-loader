package test;

import loader.DataLoader;
import graql.lang.statement.Statement;

import java.io.*;
import java.util.ArrayList;

public class TestUtil {

    public static ArrayList<String> getData(String path) {
        ArrayList<String> rows = new ArrayList<>();
        InputStream es = DataLoader.getInputStream(path);
        if (es != null) {
            String line;
            try (BufferedReader br = new BufferedReader(new InputStreamReader(es))) {
                while ((line = br.readLine()) != null) {
                    rows.add(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rows;
    }

    public static String concatMatches(ArrayList<Statement> statements) {
        String ret = "";
        for (Statement st :
                statements) {
            ret = ret + st.toString();
        }
        return ret;
    }
}
