/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms.mirror.feature;


import org.apache.commons.text.StringEscapeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class BaseFeature {

    protected Boolean contains(String search, List<String> definition) {
        Boolean rtn = Boolean.FALSE;
        for (String line : definition) {
            if (line.trim().toUpperCase(Locale.ROOT).startsWith(search.toUpperCase(Locale.ROOT))) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    protected String getGroupFor(Pattern pattern, List<String> definition) {
        String rtn = null;
        for (String line : definition) {
//            String adjLine = StringEscapeUtils.escapeJava(line);
            Matcher m = pattern.matcher(line);
            if (m.find()) {
                rtn = m.group(1);
            }
        }
        return rtn;
    }

    protected int indexOf(List<String> definition, String condition) {
        int rtn = -1;
        int loc = 0;
        for (String line : definition) {
            if (line.trim().toUpperCase(Locale.ROOT).startsWith(condition.toUpperCase(Locale.ROOT))) {
                rtn = loc;
                break;
            } else {
                loc++;
            }
        }
        return rtn;
    }

    protected Boolean removeRange(int from, int to, List<String> definition) {
        Boolean rtn = Boolean.FALSE;
        if (from < to) {
            for (int i = from; i < to; i++) {
                ((ArrayList)definition).remove(from);
            }
        }
        return rtn;
    }

//    public List<String> addEscaped(List<String> definition) {
//        List<String> escapedList = new ArrayList<String>();
//        for (String line : definition) {
//            // Causing too many level of escapes.
////            if (line.contains("escape.delim")) {
////                String escapedLine = StringEscapeUtils.escapeJava(line);
////                escapedList.add(escapedLine);
////            } else {
//                escapedList.add(line);
////            }
//        }
//        return escapedList;
//    }
}
