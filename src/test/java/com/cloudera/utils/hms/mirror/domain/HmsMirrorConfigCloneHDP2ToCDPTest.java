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

package com.cloudera.utils.hms.mirror.domain;

import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.utils.ConfigTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@Slf4j
public class HmsMirrorConfigCloneHDP2ToCDPTest {

    private HmsMirrorConfig hmsMirrorConfig;

    @Before
    public void setUp() throws IOException {
        log.info("Setting up HmsMirrorConfigCloneTest");
        hmsMirrorConfig = ConfigTest.deserializeResource("/config/default.yaml.hdp2-cdp");
    }

    @Test
    public void cloneTest_01() {
        log.info("Test 01");
        HmsMirrorConfig clone = hmsMirrorConfig.clone();

        // Add your assertions here
        assertEquals(clone.getHybrid().getSqlPartitionLimit(), hmsMirrorConfig.getHybrid().getSqlPartitionLimit());
        assertEquals(clone.getHybrid().getSqlSizeLimit(), hmsMirrorConfig.getHybrid().getSqlSizeLimit());

        assertEquals(clone.getCluster(Environment.LEFT).isLegacyHive(), hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive());
        assertEquals(clone.getCluster(Environment.RIGHT).isLegacyHive(), hmsMirrorConfig.getCluster(Environment.RIGHT).isLegacyHive());

        assertEquals(clone.getCluster(Environment.LEFT).getHcfsNamespace(), hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace());
        assertEquals(clone.getCluster(Environment.RIGHT).getHcfsNamespace(), hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace());

        assertEquals(clone.getCluster(Environment.RIGHT).getPartitionDiscovery().isInitMSCK(), hmsMirrorConfig.getCluster(Environment.RIGHT).getPartitionDiscovery().isInitMSCK());
        assertEquals(clone.getCluster(Environment.RIGHT).getPartitionDiscovery().isAuto(), hmsMirrorConfig.getCluster(Environment.RIGHT).getPartitionDiscovery().isAuto());

    }


    @Test
    public void equalsTest_101() {
        log.info("Test 01");
        HmsMirrorConfig clone = hmsMirrorConfig.clone();

        // Add your assertions here
        assertEquals(hmsMirrorConfig, clone);

    }

}
