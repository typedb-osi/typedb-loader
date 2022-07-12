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

import java.io.BufferedInputStream;
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
                return new BufferedInputStream(new GZIPInputStream(new FileInputStream(filepath)), 128_000);
            } else {
                return new BufferedInputStream(new FileInputStream(filepath), 128_000);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
