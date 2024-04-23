/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hms.mirror;

import lombok.Getter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class EnvironmentMap {

    private final Map<Environment, Set<TranslationLevel>> environmentMap = new TreeMap<>();

    public void addTranslationLocation(Environment environment, String original, String target, int level) {
        Set<TranslationLevel> dbTranslationSet = environmentMap.computeIfAbsent(environment, k -> new HashSet<>());
        dbTranslationSet.add(new TranslationLevel(original, target, level));
    }

    public Set<TranslationLevel> getTranslationSet(Environment environment) {
        Set<TranslationLevel> dbTranslationSet = environmentMap.computeIfAbsent(environment, k -> new HashSet<>());
        return dbTranslationSet;
    }


    @Getter
    public static class TranslationLevel {
        final int level;
        final String original, target;

        public TranslationLevel(String original, String target, int level) {
            this.original = original;
            this.target = target;
            this.level = level;
        }

        public String getAdjustedOriginal() {
            String rtn = Translator.reduceUrlBy(original, level);
            return rtn;
        }

        public String getAdjustedTarget() {
            String rtn = Translator.reduceUrlBy(target, level);
            return rtn;
        }
    }

}
