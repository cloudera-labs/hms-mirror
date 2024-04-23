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

import com.cloudera.utils.hms.mirror.service.TransferService;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;

@Slf4j
@Getter
@Setter
@JsonIgnoreProperties({"dbLocationMap"})
public class Translator {

    @JsonIgnore
    private final Map<String, EnvironmentMap> dbLocationMap = new TreeMap<>();
    @JsonIgnore
    private HmsMirrorConfig hmsMirrorConfig;
    /*
    Use this to force the location element in the external table create statements and
    not rely on the database 'location' element.
     */
    private boolean forceExternalLocation = Boolean.FALSE;
    private Map<String, String> globalLocationMap = null;

    @JsonIgnore
    private Map<String, String> orderedGlobalLocationMap = null;

    public static String getLastDirFromUrl(final String urlString) {
        Matcher matcher = TransferService.lastDirPattern.matcher(urlString);
        if (matcher.find()) {
            String matchStr = matcher.group(1);
            // Remove last occurrence ONLY.
            int lastIndexOf = urlString.lastIndexOf(matchStr);
            return urlString.substring(lastIndexOf, urlString.length());
        } else {
            return urlString;
        }
    }

    public static String removeLastDirFromUrl(final String url) {
        Matcher matcher = TransferService.lastDirPattern.matcher(url);
        if (matcher.find()) {
            String matchStr = matcher.group(1);
            // Remove last occurrence ONLY.
            Integer lastIndexOf = url.lastIndexOf(matchStr);
            return url.substring(0, lastIndexOf - 1);
        } else {
            return url;
        }
    }

//    public void setGlobalLocationMap(Map<String, String> globalLocationMap) {
//        getGlobalLocationMap().putAll(globalLocationMap);
//    }

    public static String replaceLast(String text, String regex, String replacement) {
        return text.replaceFirst("(?s)" + regex + "(?!.*?" + regex + ")", replacement);
    }

    public static String reduceUrlBy(String url, int level) {
        String rtn = url.trim();
        if (rtn.endsWith("/"))
            rtn = rtn.substring(0, rtn.length() - 2);
        for (int i = 0; i < level; i++) {
            rtn = removeLastDirFromUrl(rtn);
        }
        return rtn;
    }

    public void addGlobalLocationMap(String from, String to) {
        getOrderedGlobalLocationMap().put(from, to);
    }

    public synchronized void addLocation(String database, Environment environment, String originalLocation, String newLocation, int level) {
        EnvironmentMap environmentMap = dbLocationMap.computeIfAbsent(database, k -> new EnvironmentMap());
        environmentMap.addTranslationLocation(environment, originalLocation, newLocation, level);
//        getDbLocationMap(database, environment).put(originalLocation, newLocation);
    }

    public synchronized Set<EnvironmentMap.TranslationLevel> getDbLocationMap(String database, Environment environment) {
        EnvironmentMap envMap = dbLocationMap.computeIfAbsent(database, k -> new EnvironmentMap());
        return envMap.getTranslationSet(environment);
    }

    @JsonIgnore
    // This set is ordered by the length of the key in descending order
    // to ensure that the longest path is replaced first.
    public Map<String, String> getOrderedGlobalLocationMap() {
        if (orderedGlobalLocationMap == null) {
            Comparator<String> stringLengthComparator = new Comparator<String>() {
                // return comparison of two strings, first by length then by value.
                public int compare(String k1, String k2) {
                    int comp = 0;
                    if (k1.length() > k2.length()) {
                        comp = -1;
                    } else if (k1.length() == k2.length()) {
                        comp = 0;
                    } else {
                        comp = 1;
                    }
                    if (comp == 0)
                        comp = k1.compareTo(k2);
                    return comp;
                }
            };
            orderedGlobalLocationMap = new TreeMap<String, String>(stringLengthComparator);
            // Add the global location map to the ordered map.
            if (globalLocationMap != null)
                orderedGlobalLocationMap.putAll(globalLocationMap);
        }
        return orderedGlobalLocationMap;
    }

    public Boolean validate() {
        Boolean rtn = Boolean.TRUE;
        return rtn;
    }

}
