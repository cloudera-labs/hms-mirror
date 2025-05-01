/*
 * Copyright (c) 2022-2023. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.features;

import com.cloudera.utils.hms.mirror.feature.Feature;
import com.cloudera.utils.hms.mirror.feature.FeaturesEnum;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class FeaturesCombinedTest {

    public FeaturesEnum doit(List<String> schema) {
        FeaturesEnum appliedFeature = null;
        for (FeaturesEnum features : FeaturesEnum.values()) {
            Feature feature = features.getFeature();
            if (feature.applicable(schema)) {
                System.out.println("Adjusting: " + feature.getDescription());
                if (feature.fixSchema(schema)) {
                    if (appliedFeature != null) {
                        assertFalse(Boolean.TRUE, "Feature: " + appliedFeature + " was already applied." +
                                "Now attempting to applied second feature: " + features);
                    }
                    appliedFeature = features;
                }
            }
        }
        return appliedFeature;
    }

    @Test
    public void test_orc_003() {
        List<String> schema = toList(BadOrcDefFeatureTest.schema_01);
        FeaturesEnum check = doit(schema);
        assertEquals(FeaturesEnum.BAD_ORC_DEF, check);
        schema.stream().forEach(System.out::println);
    }

    @Test
    public void test_orc_004() {
        List<String> schema = toList(BadOrcDefFeatureTest.schema_02);
        FeaturesEnum check = doit(schema);
        assertNull(check);
        schema.stream().forEach(System.out::println);
    }

    @Test
    public void test_parquet_001() {
        List<String> schema = toList(BadParquetDefFeatureTest.schema_01);
        FeaturesEnum check = doit(schema);
        assertEquals(FeaturesEnum.BAD_PARQUET_DEF, check);
        schema.stream().forEach(System.out::println);
    }

    @Test
    public void test_parquet_002() {
        List<String> schema = toList(BadParquetDefFeatureTest.schema_02);
        FeaturesEnum check = doit(schema);
        assertEquals(FeaturesEnum.BAD_PARQUET_DEF, check);
        schema.stream().forEach(System.out::println);
    }

    @Test
    public void test_rc_005() {
        List<String> schema = toList(BadRCDefFeatureTest.schema_01);
        FeaturesEnum check = doit(schema);
        assertEquals(FeaturesEnum.BAD_RC_DEF, check);
        schema.stream().forEach(System.out::println);
    }

    @Test
    public void test_textfile_007() {
        List<String> schema = toList(BadTextfileDefFeatureTest.schema_01);
        FeaturesEnum check = doit(schema);
        assertEquals(FeaturesEnum.BAD_TEXTFILE_DEF, check);
        schema.stream().forEach(System.out::println);
    }

    @Test
    public void test_textfile_008() {
        List<String> schema = toList(BadTextfileDefFeatureTest.schema_02);
        FeaturesEnum check = doit(schema);
        assertEquals(FeaturesEnum.BAD_TEXTFILE_DEF, check);
        schema.stream().forEach(System.out::println);
    }

    private List<String> toList(String[] array) {
        List<String> rtn = new ArrayList<>();
        Collections.addAll(rtn, array);
        return rtn;
    }

}
