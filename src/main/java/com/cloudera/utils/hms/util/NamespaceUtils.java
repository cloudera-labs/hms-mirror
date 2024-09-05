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

import lombok.extern.slf4j.Slf4j;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Slf4j
public class NamespaceUtils {

    public static final Pattern protocolNSPattern = Pattern.compile("(^.*://)(\\w*(?:(?:[a-zA-Z0-9-@]*|(?<!-)\\.(?![-.]))*[a-zA-Z0-9]+)?)(:\\d{4})?");
    public static final Pattern lastDirPattern = Pattern.compile(".*/([^/?]+).*");

    public static String getNamespace(String locationWithNamespace) {
        String rtn = null;
        if (!isBlank(locationWithNamespace)) {
            log.trace("Location with namespace: {}", locationWithNamespace);
            // Find the protocol (namespace
            Matcher matcher = protocolNSPattern.matcher(locationWithNamespace);
            if (matcher.find()) {
                StringBuilder sb = new StringBuilder();
                log.trace("{} protocol found.", locationWithNamespace);
                for (int i = 1; i <= matcher.groupCount(); i++) {
                    log.trace("Parts of Location String {}: {}", i, matcher.group(i));
                    // Construct the Namespace from the found elements, until null.
                    if (nonNull(matcher.group(i))) {
                        sb.append(matcher.group(i));
                    }
                }
                // Return the namespace
                rtn = sb.toString();
            }
        }
        return rtn;
    }

    public static String getProtocol(String locationWithNamespace) {
        String rtn = null;
        if (!isBlank(locationWithNamespace)) {
            log.trace("Location with namespace: {}", locationWithNamespace);
            // Find the protocol (namespace
            Matcher matcher = protocolNSPattern.matcher(locationWithNamespace);
            if (matcher.find()) {
                StringBuilder sb = new StringBuilder();
                log.trace("{} protocol found.", locationWithNamespace);
                if (nonNull(matcher.group(1))) {
                    sb.append(matcher.group(1));
                }
                // Return the namespace
                rtn = sb.toString();
            }
        }
        return rtn;
    }

    public static String stripNamespace(String locationWithNamespace) {
        String namespace = getNamespace(locationWithNamespace);
        return nonNull(namespace) ? locationWithNamespace.replace(namespace, "") : locationWithNamespace;
    }

    public static String replaceNamespace(String locationWithNamespace, String newNamespace) {
        String relativePath = stripNamespace(locationWithNamespace);
        // Ensure relative path starts with a slash.
        if (!relativePath.startsWith("/")) {
            relativePath = "/" + relativePath;
        }
        // Strip trailing slash
        if (newNamespace.endsWith("/")) {
            newNamespace = newNamespace.substring(0, newNamespace.length() - 1);
        }
        return !isBlank(newNamespace) ? newNamespace + relativePath : relativePath;
    }

    public static String getLastDirectory(String location) {
        Matcher matcher = lastDirPattern.matcher(location);
        return matcher.find() ? matcher.group(1) : null;
    }

    // Get the parent directory of the location by stripping off the last directory.
    public static String getParentDirectory(String location) {
        // Ensure the path for the right exists.
        String lastDirectory = getLastDirectory(location.trim());
        String parentDirectory = null;
        int extra = 1;
        if (location.trim().endsWith("/")) {
            extra += 1;
        }
        parentDirectory = location.trim().substring(0, location.trim().length() - (lastDirectory.length() + extra));

        return parentDirectory;// != null? location.substring(0, location.length() - (lastDirectory.length() + 1)): null;
    }
}
