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

package util;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class QueryUtilTest {
    public static LocalDateTime getDT(String dtString) {
        DateTimeFormatter isoDateFormatter = DateTimeFormatter.ISO_DATE;
        String[] dt = dtString.split("T");
        LocalDate date = LocalDate.parse(dt[0], isoDateFormatter);
        if (dt.length > 1) {
            LocalTime time = LocalTime.parse(dt[1], DateTimeFormatter.ISO_TIME);
            return date.atTime(time);
        } else {
            return date.atStartOfDay();
        }
    }
}
