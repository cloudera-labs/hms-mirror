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

package com.cloudera.utils.hms.mirror.utils;

import com.cloudera.utils.hms.util.NamespaceUtils;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NamespaceUtilsTest {

    @Test
    public void testRetrieveNamespace() {
        // Test with namespace
        String locationWithNamespace = "hdfs://nameservice1/user/hive/warehouse";
        String namespace = NamespaceUtils.getNamespace(locationWithNamespace);
        assertEquals("hdfs://nameservice1", namespace);

        // Test with namespace
        String locationWithNamespace1 = "hdfs://nameservice1:8020/user/hive/warehouse";
        String namespace1 = NamespaceUtils.getNamespace(locationWithNamespace1);
        assertEquals("hdfs://nameservice1:8020", namespace1);

        // Test with namespace
        String locationWithNamespace10 = "hdfs://name_service1:8020/user/hive/warehouse";
        String namespace10 = NamespaceUtils.getNamespace(locationWithNamespace10);
        assertEquals("hdfs://name_service1:8020", namespace10);

        // Test with namespace
        String locationWithNamespace2 = "hdfs://CAPSNAMESPACE/user/hive/warehouse";
        String namespace2 = NamespaceUtils.getNamespace(locationWithNamespace2);
        assertEquals("hdfs://CAPSNAMESPACE", namespace2);

        // Test with namespace
        String locationWithNamespace3 = "hdfs://CAPS_NAMESPACE/user/hive/warehouse";
        String namespace3 = NamespaceUtils.getNamespace(locationWithNamespace3);
        assertEquals("hdfs://CAPS_NAMESPACE", namespace3);

        // Test with namespace
        String locationWithNamespace4 = "ofs://CAPS_NAMESPACE/user/hive/warehouse";
        String namespace4 = NamespaceUtils.getNamespace(locationWithNamespace4);
        assertEquals("ofs://CAPS_NAMESPACE", namespace4);

        // Test with namespace
        String locationWithNamespace5 = "s3a://BUCKET/user/hive/warehouse";
        String namespace5 = NamespaceUtils.getNamespace(locationWithNamespace5);
        assertEquals("s3a://BUCKET", namespace5);

        // Test without namespace
        locationWithNamespace = "hdfs://user/hive/warehouse";
        namespace = NamespaceUtils.getNamespace(locationWithNamespace);
        assertEquals("hdfs://user", namespace);

        // Test with empty namespace
        locationWithNamespace = "hdfs:///user/hive/warehouse";
        namespace = NamespaceUtils.getNamespace(locationWithNamespace);
        assertEquals("hdfs://", namespace);

        // Test with empty namespace
        String locationWithNamespace6 = "/user/hive/warehouse";
        String namespace6 = NamespaceUtils.getNamespace(locationWithNamespace6);
        assertEquals(null, namespace6);

        // Test with null namespace
        locationWithNamespace = null;
        namespace = NamespaceUtils.getNamespace(locationWithNamespace);
        assertEquals(null, namespace);
    }

    @Test
    public void testStripNamespace() {
        // Test with namespace
        String locationWithNamespace = "hdfs://nameservice1/user/hive/warehouse";
        String strippedLocation = NamespaceUtils.stripNamespace(locationWithNamespace);
        assertEquals("/user/hive/warehouse", strippedLocation);

        // Test with namespace
        String locationWithNamespace1 = "hdfs://nameservice1:8020/user/hive/warehouse";
        String strippedLocation1 = NamespaceUtils.stripNamespace(locationWithNamespace1);
        assertEquals("/user/hive/warehouse", strippedLocation1);

        // Test with namespace
        String locationWithNamespace2 = "hdfs://CAPSNAMESPACE/user/hive/warehouse";
        String strippedLocation2 = NamespaceUtils.stripNamespace(locationWithNamespace2);
        assertEquals("/user/hive/warehouse", strippedLocation2);

        // Test with namespace
        String locationWithNamespace3 = "hdfs://CAPS_NAMESPACE/user/hive/warehouse";
        String strippedLocation3 = NamespaceUtils.stripNamespace(locationWithNamespace3);
        assertEquals("/user/hive/warehouse", strippedLocation3);

        // Test without namespace
        locationWithNamespace = "hdfs://user/hive/warehouse";
        strippedLocation = NamespaceUtils.stripNamespace(locationWithNamespace);
        assertEquals("/hive/warehouse", strippedLocation);


        // Test with empty namespace
        locationWithNamespace = "/user/hive/warehouse";
        strippedLocation = NamespaceUtils.stripNamespace(locationWithNamespace);
        assertEquals("/user/hive/warehouse", strippedLocation);

        // Test with empty namespace
        locationWithNamespace = "user/hive/warehouse";
        strippedLocation = NamespaceUtils.stripNamespace(locationWithNamespace);
        assertEquals("user/hive/warehouse", strippedLocation);

        // Test with null namespace
        locationWithNamespace = null;
        strippedLocation = NamespaceUtils.stripNamespace(locationWithNamespace);
        assertEquals(null, strippedLocation);
    }

    @Test
    public void testReplaceNamespace() {
        // Test with namespace
        String locationWithNamespace = "hdfs://nameservice1/user/hive/warehouse";
        String newNamespace = "hdfs://nameservice2";
        String replacedLocation = NamespaceUtils.replaceNamespace(locationWithNamespace, newNamespace);
        assertEquals("hdfs://nameservice2/user/hive/warehouse", replacedLocation);

        // Test with namespace
        String locationWithNamespace1 = "hdfs://nameservice1:8020/user/hive/warehouse";
        String newNamespace1 = "hdfs://nameservice2";
        String replacedLocation1 = NamespaceUtils.replaceNamespace(locationWithNamespace1, newNamespace1);
        assertEquals("hdfs://nameservice2/user/hive/warehouse", replacedLocation1);

        // Test with namespace
        String locationWithNamespace2 = "hdfs://CAPSNAMESPACE/user/hive/warehouse";
        String newNamespace2 = "hdfs://CAPSNAMESPACE2";
        String replacedLocation2 = NamespaceUtils.replaceNamespace(locationWithNamespace2, newNamespace2);
        assertEquals("hdfs://CAPSNAMESPACE2/user/hive/warehouse", replacedLocation2);

        // Test with namespace
        String locationWithNamespace3 = "hdfs://CAPS_NAMESPACE/user/hive/warehouse";
        String newNamespace3 = "hdfs://CAPS_NAMESPACE2";
        String replacedLocation3 = NamespaceUtils.replaceNamespace(locationWithNamespace3, newNamespace3);
        assertEquals("hdfs://CAPS_NAMESPACE2/user/hive/warehouse", replacedLocation3);

        // Test with namespace
        String locationWithNamespace4 = "hdfs://nameservice1:8020/user/hive/warehouse";
        String newNamespace4 = "ofs://CAPS_NAMESPACE2";
        String replacedLocation4 = NamespaceUtils.replaceNamespace(locationWithNamespace4, newNamespace4);
        assertEquals("ofs://CAPS_NAMESPACE2/user/hive/warehouse", replacedLocation4);

        // Test with namespace
        String locationWithNamespace5 = "hdfs://nameservice1/user/hive/warehouse";
        String newNamespace5 = "s3a://BUCKET2";
        String replacedLocation5 = NamespaceUtils.replaceNamespace(locationWithNamespace5, newNamespace5);
        assertEquals("s3a://BUCKET2/user/hive/warehouse", replacedLocation5);

    }

    @Test
    public void testLastDirectory() {
        // Test with namespace
        String locationWithNamespace = "hdfs://nameservice1/user/hive/warehouse";
        String lastDir = NamespaceUtils.getLastDirectory(locationWithNamespace);
        assertEquals("warehouse", lastDir);

        // Test with namespace
        String locationWithNamespace1 = "hdfs://nameservice1:8020/user/hive/warehouse";
        String lastDir1 = NamespaceUtils.getLastDirectory(locationWithNamespace1);
        assertEquals("warehouse", lastDir1);

        // Test with namespace
        String locationWithNamespace2 = "hdfs://CAPSNAMESPACE/user/hive/warehouse";
        String lastDir2 = NamespaceUtils.getLastDirectory(locationWithNamespace2);
        assertEquals("warehouse", lastDir2);

        // Test with namespace
        String locationWithNamespace3 = "hdfs://CAPS_NAMESPACE/user/hive/warehouse";
        String lastDir3 = NamespaceUtils.getLastDirectory(locationWithNamespace3);
        assertEquals("warehouse", lastDir3);

        // Test with namespace
        String locationWithNamespace4 = "hdfs://nameservice1:8020/user/hive/warehouse/";
        String lastDir4 = NamespaceUtils.getLastDirectory(locationWithNamespace4);
        assertEquals("warehouse", lastDir4);

        // Test with namespace
        String locationWithNamespace5 = "hdfs://nameservice1/user/hive/warehouse/";
        String lastDir5 = NamespaceUtils.getLastDirectory(locationWithNamespace5);
        assertEquals("warehouse", lastDir5);

        // Test with namespace
        String locationWithNamespace6 = "hdfs://nameservice1/user/hive/warehouse/table";
        String lastDir6 = NamespaceUtils.getLastDirectory(locationWithNamespace6);
        assertEquals("table", lastDir6);

        // Test with namespace
        String locationWithNamespace7 = "hdfs://nameservice1/user/hive/warehouse/table/";
        String lastDir7 = NamespaceUtils.getLastDirectory(locationWithNamespace7);
        assertEquals("table", lastDir7);

        // Test with namespace
        String locationWithNamespace8 = "hdfs://nameservice1/user/hive/warehouse/table/partition";
        String lastDir8 = NamespaceUtils.getLastDirectory(locationWithNamespace8);
        assertEquals("partition", lastDir8);

    }

    @Test
    public void testParentDirectory() {
        // Test with namespace
        String locationWithNamespace = "hdfs://nameservice1/user/hive/warehouse";
        String parentDir = NamespaceUtils.getParentDirectory(locationWithNamespace);
        assertEquals("hdfs://nameservice1/user/hive", parentDir);

        // Test with namespace
        String locationWithNamespace1 = "hdfs://nameservice1:8020/user/hive/warehouse";
        String parentDir1 = NamespaceUtils.getParentDirectory(locationWithNamespace1);
        assertEquals("hdfs://nameservice1:8020/user/hive", parentDir1);

        // Test with namespace
        String locationWithNamespace2 = "hdfs://CAPSNAMESPACE/user/hive/warehouse";
        String parentDir2 = NamespaceUtils.getParentDirectory(locationWithNamespace2);
        assertEquals("hdfs://CAPSNAMESPACE/user/hive", parentDir2);

        // Test with namespace
        String locationWithNamespace3 = "hdfs://CAPS_NAMESPACE/user/hive/warehouse";
        String parentDir3 = NamespaceUtils.getParentDirectory(locationWithNamespace3);
        assertEquals("hdfs://CAPS_NAMESPACE/user/hive", parentDir3);

        // Test with namespace
        String locationWithNamespace4 = "hdfs://nameservice1:8020/user/hive/warehouse/";
        String parentDir4 = NamespaceUtils.getParentDirectory(locationWithNamespace4);
        assertEquals("hdfs://nameservice1:8020/user/hive", parentDir4);

        // Test with namespace
        String locationWithNamespace5 = "hdfs://nameservice1/user/hive/warehouse/";
        String parentDir5 = NamespaceUtils.getParentDirectory(locationWithNamespace5);
        assertEquals("hdfs://nameservice1/user/hive", parentDir5);

        // Test with namespace
        String locationWithNamespace6 = "hdfs://nameservice1/user/hive/warehouse/table";
        String parentDir6 = NamespaceUtils.getParentDirectory(locationWithNamespace6);
        assertEquals("hdfs://nameservice1/user/hive/warehouse", parentDir6);

        // Test with namespace
        String locationWithNamespace7 = "hdfs://nameservice1/user/hive/warehouse/table/";
        String parentDir7 = NamespaceUtils.getParentDirectory(locationWithNamespace7);
        assertEquals("hdfs://nameservice1/user/hive/warehouse", parentDir7);

        // Test with namespace
        String locationWithNamespace8 = "hdfs://nameservice1/user/hive/warehouse/table/partition";
        String parentDir8 = NamespaceUtils.getParentDirectory(locationWithNamespace8);
        assertEquals("hdfs://nameservice1/user/hive/warehouse/table", parentDir8);
    }
}
