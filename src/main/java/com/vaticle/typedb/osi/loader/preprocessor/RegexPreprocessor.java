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

package com.vaticle.typedb.osi.loader.preprocessor;

public class RegexPreprocessor {
    String match;
    String replace;

    public RegexPreprocessor(String match, String replace) {
        this.match = match;
        this.replace = replace;
    }

    public String applyProcessor(String value) {
        return value.replaceAll(match, replace);
    }

}
