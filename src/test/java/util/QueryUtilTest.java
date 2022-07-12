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

package com.vaticle.typedb.osi.util;

import com.vaticle.typedb.client.api.TypeDBSession;
import com.vaticle.typedb.osi.config.Configuration;

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
    public static void setPlayerAttributeTypes(Configuration.Generator.Relation relation, int playerIndex, TypeDBSession session) {
        if (relation.getInsert().getPlayers()[playerIndex].getMatch() != null) {
            Configuration.Definition.Thing currentPlayerMatch = relation.getInsert().getPlayers()[playerIndex].getMatch();
            if (currentPlayerMatch != null) {
                // if attribute player
                if (currentPlayerMatch.getAttribute() != null) {
                    System.out.println("attribute player, index " + playerIndex + " " + relation.getInsert().getRelation());
                    currentPlayerMatch.getAttribute().setAttribute(currentPlayerMatch.getType());
                    currentPlayerMatch.getAttribute().setConceptValueType(session);
                }
                // if byAttribute player
                else if (currentPlayerMatch.getOwnerships() != null) {
                    System.out.println("byAttribute player, index " + playerIndex + " " + relation.getInsert().getRelation());
                    for (Configuration.Definition.Attribute ownership : currentPlayerMatch.getOwnerships()) {
                        ownership.setConceptValueType(session);
                    }
                }
            }
        }
    }
}
