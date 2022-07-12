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

import org.junit.Test;

public class ErrorLoggerTest {

    @Test
    public void errorFileLogger() {
        FileLogger logger = FileLogger.getLogger();
        logger.logMalformed("entities.tsv", "shucks! There was a malformed row error");
        logger.logUnavailable("entities.tsv", "shucks! There was a connection error");
        logger.logInvalid("entities.tsv", "shucks! There was a invalid row error");
    }

}
