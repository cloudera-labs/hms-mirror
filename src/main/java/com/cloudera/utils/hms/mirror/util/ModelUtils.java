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

package com.cloudera.utils.hms.mirror.util;

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hms.mirror.domain.support.ConnectionPoolType;
import com.cloudera.utils.hms.mirror.domain.support.DataMovementStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.web.controller.ControllerReferences;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ModelUtils implements ControllerReferences {

    public static void allEnumsForMap(DataStrategyEnum dataStrategy, Map<String, Object> map) {
        enumForMap(com.cloudera.utils.hms.mirror.domain.support.SerdeType.class, map);
        enumForMap(com.cloudera.utils.hms.mirror.domain.support.TableType.class, map);
        enumForMap(com.cloudera.utils.hms.mirror.domain.support.StageEnum.class, map);
        enumForMap(com.cloudera.utils.hms.mirror.domain.support.CollectionEnum.class, map);
        enumForMap(com.cloudera.utils.hms.mirror.domain.support.SideType.class, map);
        enumForMap(com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum.class, map);
        enumForMap(com.cloudera.utils.hms.mirror.domain.support.DistcpFlowEnum.class, map);
        enumForMap(com.cloudera.utils.hms.mirror.domain.support.TranslationTypeEnum.class, map);
//        enumForMap(DBStore.DB_TYPE.class, map);
        configSupportedDBType(map);
        configSupportDataMovementStrategyForModel(dataStrategy, map);
        configEnvironmentForModel(dataStrategy, map);
        configSupportDataStrategyForModel(map);
        configSupportedHiveDriverClassesForModel(map);
        enumForMap(ConnectionPoolType.class, map);
        map.put("FALSE", "false");
        map.put("TRUE", "true");
        booleanForModel(map);

    }

    public static void configSupportedDBType(Map<String, Object> map) {
        map.put("db_types", Arrays.asList(DBStore.DB_TYPE.MYSQL, DBStore.DB_TYPE.POSTGRES, DBStore.DB_TYPE.ORACLE));
    }

    public static void configEnvironmentForModel(DataStrategyEnum dataStrategy, Map<String, Object> map) {
        // Add LEFT and RIGHT to the model
        switch (dataStrategy) {
            case STORAGE_MIGRATION:
            case DUMP:
                map.put(ENVIRONMENTS, Arrays.asList("LEFT"));
                break;
            default:
                map.put(ENVIRONMENTS, new String[]{"LEFT", "RIGHT"});
                break;
        }
    }

    public static List<DataStrategyEnum> getSupportedDataStrategies() {
        List<DataStrategyEnum> supportedDataStrategies = new ArrayList<>();
        supportedDataStrategies.add(DataStrategyEnum.STORAGE_MIGRATION);
        supportedDataStrategies.add(DataStrategyEnum.DUMP);
        supportedDataStrategies.add(DataStrategyEnum.SCHEMA_ONLY);
        supportedDataStrategies.add(DataStrategyEnum.SQL);
        supportedDataStrategies.add(DataStrategyEnum.EXPORT_IMPORT);
        supportedDataStrategies.add(DataStrategyEnum.HYBRID);
        supportedDataStrategies.add(DataStrategyEnum.COMMON);
        supportedDataStrategies.add(DataStrategyEnum.LINKED);

        return supportedDataStrategies;
    }

    public static void configSupportDataStrategyForModel(Map<String, Object> map) {
        map.put(SUPPORTED_DATA_STRATEGIES, getSupportedDataStrategies().toArray(new DataStrategyEnum[0]));
    }

    public static void configSupportDataMovementStrategyForModel(DataStrategyEnum dataStrategy, Map<String, Object> map) {
        switch (dataStrategy) {
            case STORAGE_MIGRATION:
                map.put("datamovementstrategyenums", Arrays.asList(DataMovementStrategyEnum.DISTCP, DataMovementStrategyEnum.SQL));
                break;
            case SCHEMA_ONLY:
                map.put("datamovementstrategyenums", Arrays.asList(DataMovementStrategyEnum.DISTCP, DataMovementStrategyEnum.MANUAL));
                break;
            default:
                enumForMap(DataMovementStrategyEnum.class, map);
                break;
        }
    }

    public static void configSupportedHiveDriverClassesForModel(Map<String, Object> map) {
        // Add SUPPORTED and UNSUPPORTED to the model
        map.put(SUPPORTED_HIVE_DRIVER_CLASSES,
                new String[]{"org.apache.hive.jdbc.HiveDriver", "com.cloudera.hive.jdbc.HS2Driver"});
    }

    public static void enumForMap(Class clazz, Map<String, Object> map) {
        if (clazz.isEnum()) {
            Method method = null;
            try {
                method = clazz.getMethod("values");
                Enum<?>[] enums = (Enum<?>[]) method.invoke(null);
                String[] enumNames = new String[enums.length];
                for (int i = 0; i < enums.length; i++) {
                    enumNames[i] = enums[i].name();
                }
                map.put(clazz.getSimpleName().toLowerCase() + "s", enumNames);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void booleanForModel(Map<String, Object> map) {
        String[] bools = new String[2];
        bools[0] = "false";
        bools[1] = "true";
        map.put(BOOLEANS, bools);
    }

}
