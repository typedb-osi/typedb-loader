package util;

import java.io.*;

public class Util {
    public static String getAbsPath(String p) {
        File file = new File(p);
        return file.getAbsolutePath();
    }
}
