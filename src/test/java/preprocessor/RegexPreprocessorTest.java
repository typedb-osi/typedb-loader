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

package preprocessor;

import preprocessor.RegexPreprocessor;
import org.junit.Assert;
import org.junit.Test;

public class RegexPreprocessorTest {

    @Test
    public void regexPreprocessorTest() {
        RegexPreprocessor rpp = new RegexPreprocessor("^.*(fakebook\\.com.*)/$", "$1");
        String test = "https://www.fakebook.com/personOne/";
        String res = "fakebook.com/personOne";
        Assert.assertEquals(res, rpp.applyProcessor(test));

        test = "www.fakebook.com/personOne/";
        res = "fakebook.com/personOne";
        Assert.assertEquals(res, rpp.applyProcessor(test));

        test = "fakebook.com/personOne/";
        res = "fakebook.com/personOne";
        Assert.assertEquals(res, rpp.applyProcessor(test));

        test = "personOne/";
        res = "personOne/";
        Assert.assertEquals(res, rpp.applyProcessor(test));

        test = "insertedWithoutAppliedRegex";
        res = "insertedWithoutAppliedRegex";
        Assert.assertEquals(res, rpp.applyProcessor(test));

        test = "";
        res = "";
        Assert.assertEquals(res, rpp.applyProcessor(test));
    }

}