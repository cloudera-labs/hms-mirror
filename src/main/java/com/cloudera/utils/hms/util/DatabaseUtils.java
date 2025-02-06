/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Objects.nonNull;

public class DatabaseUtils {

    public static Map<String, String> parametersToMap(String parametersStr) {
        Map<String, String> map = new HashMap<>();
        // Trim the brackets
        if (nonNull(parametersStr)) {
            String lclParameters = parametersStr.trim().substring(1, parametersStr.length() - 1);
            for (String parameter : lclParameters.split(",")) {
                String[] parts = parameter.split("=");
                if (parts.length == 2) {
                    map.put(parts[0], parts[1]);
                }
            }
        }
        return map;
    }

    public static boolean upsertParameters(Map<String, String> source, Map<String, String> target, Set<String> skipList) {
        boolean changed = false;
        for (Map.Entry<String, String> entry : source.entrySet()) {
            if (skipList.contains(entry.getKey())) {
                continue;
            }
            if (!target.containsKey(entry.getKey()) || !target.get(entry.getKey()).equals(entry.getValue())) {
                target.put(entry.getKey(), entry.getValue());
                changed = true;
            }
        }
        return changed;
    }

    public static Map<String, String> getParameters(Map<String, String> parameters, Set<String> staticSkipList, Set<Pattern> dynamicSkipList) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String gKey = entry.getKey().trim().replaceAll("^\"|\"$", "");
            gKey = gKey.replaceAll("^'|'$", "");
            if (!staticSkipList.contains(gKey)) {
                // Now check against the user supplied dynamic skip list
                boolean skip = false;
                for (Pattern pattern : dynamicSkipList) {
                    if (pattern.matcher(gKey).matches()) {
                        skip = true;
                        break;
                    }
                }
                if (!skip) {
                    // Strip the quotes from the key and value
                    String gValue = entry.getValue().trim().replaceAll("^\"|\"$", "");
                    gValue = gValue.replaceAll("^'|'$", "");
                    result.put(gKey, gValue);
                }
            }
        }
        return result;
    }

}
