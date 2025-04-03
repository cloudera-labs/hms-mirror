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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UrlUtils {

    public static Pattern lastDirPattern = Pattern.compile(".*/([^/?]+).*");

    public static String getLastDirFromUrl(final String urlString) {
        Matcher matcher = lastDirPattern.matcher(urlString);
        if (matcher.find()) {
            String matchStr = matcher.group(1);
            // Remove last occurrence ONLY.
            int lastIndexOf = urlString.lastIndexOf(matchStr);
            return urlString.substring(lastIndexOf);
        } else {
            return urlString;
        }
    }

    public static String removeLastDirFromUrl(final String url) {
        Matcher matcher = lastDirPattern.matcher(url);
        if (matcher.find()) {
            String matchStr = matcher.group(1);
            // Remove last occurrence ONLY.
            int lastIndexOf = url.lastIndexOf(matchStr);
            return url.substring(0, lastIndexOf - 1);
        } else {
            return url;
        }
    }

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

}
