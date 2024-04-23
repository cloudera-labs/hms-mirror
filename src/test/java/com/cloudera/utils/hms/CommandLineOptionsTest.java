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

package com.cloudera.utils.hms;

import org.junit.Test;

import static com.cloudera.utils.hms.HmsMirrorCommandLineOptions.SPRING_CONFIG_PREFIX;

public class CommandLineOptionsTest {

    @Test
    public void toSpringCommandLineOptions() {
        // Test case for toSpringCommandLineOptions
        StringBuilder sb = new StringBuilder();
        for (HmsMirrorCommandLineOptionsEnum hmsMirrorCommandLineOptionsEnum : HmsMirrorCommandLineOptionsEnum.values()) {
            if (hmsMirrorCommandLineOptionsEnum.getShortName().equals("cfg")) {
                // Handle the config file differently
                sb.append("\"--hms-mirror.config-filename=\",\n");
            } else if (hmsMirrorCommandLineOptionsEnum.getShortName().equals("ltd")) {
                // Handle the ltd file differently
                sb.append("\"--hms-mirror.conversion.test-filename=\",\n");
            } else {
                sb.append("\"--").append(SPRING_CONFIG_PREFIX).append(".").append(hmsMirrorCommandLineOptionsEnum.getLongName()).append("=");
                if (hmsMirrorCommandLineOptionsEnum.getArgumentName() != null) {
                    sb.append("\",\n");
                } else {
                    sb.append("true\",\n");
                }
            }
        }
        System.out.println(sb);
    }
}
