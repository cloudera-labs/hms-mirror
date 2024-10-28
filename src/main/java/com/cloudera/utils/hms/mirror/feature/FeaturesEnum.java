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

package com.cloudera.utils.hms.mirror.feature;

import java.lang.reflect.InvocationTargetException;

public enum FeaturesEnum {
    BAD_FIELDS_FORM_FEED_DEF(BadFieldsFFDefFeature.class),
    BAD_ORC_DEF(BadOrcDefFeature.class),
    BAD_RC_DEF(BadRCDefFeature.class),
    BAD_PARQUET_DEF(BadParquetDefFeature.class),

    // Not required currently.
//    SPARK_SQL_PART_DEF(SparkSqlPartFeature.class),
    BAD_TEXTFILE_DEF(BadTextFileDefFeature.class),
    STRUCT_ESCAPE(StructEscapeFieldsFeature.class);

    private Feature feature;

    @SuppressWarnings("unchecked")
    FeaturesEnum(Class featureClass) {
        try {
            feature = (Feature) featureClass.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException | NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    public Feature getFeature() {
        return feature;
    }
}
