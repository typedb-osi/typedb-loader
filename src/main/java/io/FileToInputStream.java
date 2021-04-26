package io;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class FileToInputStream {

    public static InputStream getInputStream(String filepath) {
        try {
            if (filepath == null) {
                return null;
            } else if (filepath.endsWith(".gz")) {
                return new GZIPInputStream(new FileInputStream(filepath));
            } else {
                return new FileInputStream(filepath);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
