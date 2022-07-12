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

package com.vaticle.typedb.osi.status;


public class MigrationStatus {
    private final String conceptName;
    private boolean isCompleted;
    private int migratedRows;

    public MigrationStatus(String conceptName, boolean isCompleted, int migratedRows) {
        this.conceptName = conceptName;
        this.isCompleted = isCompleted;
        this.migratedRows = migratedRows;
    }

    public int getMigratedRows() {
        return migratedRows;
    }

    public void setMigratedRows(int migratedRows) {
        this.migratedRows = migratedRows;
    }

    public boolean isCompleted() {
        return isCompleted;
    }

    public void setCompleted(boolean completed) {
        isCompleted = completed;
    }

    @Override
    public String toString() {
        return "MigrationStatus{" +
                "conceptName='" + conceptName + '\'' +
                ", isCompleted=" + isCompleted +
                ", migratedRows=" + migratedRows +
                '}';
    }
}
