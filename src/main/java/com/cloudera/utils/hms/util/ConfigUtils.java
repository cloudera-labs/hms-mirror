package com.cloudera.utils.hms.util;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.SideType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Objects.isNull;

public class ConfigUtils {

    public final static String TEZ_QUEUE_PROPERTY = "tez.queue.name";
    public final static String MAPRED_QUEUE_PROPERTY = "mapred.job.queue.name";

    public static List<String> getPropertyOverridesFor(Environment target, HmsMirrorConfig config) {
        final ArrayList<String> overrides = new ArrayList<>();
        if (!config.getOptimization().getOverrides().getProperties().isEmpty()) {
            config.getOptimization().getOverrides().getProperties().forEach((k, v) -> {
                // Look at Map 'v' and add each key value pair as a SET statement.
                v.forEach((k1, v1) -> {
                    switch (k1) {
                        case BOTH:
                            if (!isNull(v1)) {
                                overrides.add("SET " + k + "=" + v1);
                            } else {
                                overrides.add("SET " + k );
                            }
                            break;
                        case LEFT:
                            if (target == Environment.LEFT) {
                                if (!isNull(v1)) {
                                    overrides.add("SET " + k + "=" + v1);
                                } else {
                                    overrides.add("SET " + k );
                                }
                            }
                            break;
                        case RIGHT:
                            if (target == Environment.RIGHT) {
                                if (!isNull(v1)) {
                                    overrides.add("SET " + k + "=" + v1);
                                } else {
                                    overrides.add("SET " + k );
                                }
                            }
                            break;
                    }
//                    overrides.add("SET " + k1 + "=" + v1);
                });
//                overrides.add("SET " + k + "=" + v1);
            });
        }
        return overrides;
    }

    public static String getQueuePropertyOverride(Environment environment, HmsMirrorConfig config) {
        final StringBuilder sb = new StringBuilder();
        if (!config.getOptimization().getOverrides().getProperties().isEmpty()) {
            // Create a for loop to iterate over the config.getOptimization().getOverrides().getProperties()
            // and check if the key is equal to the TEZ_QUEUE_PROPERTY or MAPRED_QUEUE_PROPERTY.
            // If it is, then check if the environment is equal to the key of the Map.
            // If it is, then append the key and value to the StringBuilder.
            // Return the StringBuilder as a String.
            for (Map.Entry<String, Map<SideType, String>> entry :
                    config.getOptimization().getOverrides().getProperties().entrySet()) {
                if (entry.getKey().toLowerCase().equals(TEZ_QUEUE_PROPERTY) ||
                        entry.getKey().toLowerCase().equals(MAPRED_QUEUE_PROPERTY)) {
                    // Look through v and see if we have a match for the environment.
                    for (Map.Entry<SideType, String> lclEntry : entry.getValue().entrySet()) {
                        if (lclEntry.getKey() == SideType.BOTH ||
                                environment.toString().equals(lclEntry.getKey().toString())) {
                            sb.append("SET ");
                            sb.append(entry.getKey().toLowerCase())
                                    .append("=")
                                    .append(lclEntry.getValue());
                            break;
                        }
                    }
                }
                // Check if this was set and break out of the loop.
                if (sb.length() > 0) {
                    break;
                }
            }
        }
        if (sb.length() > 0) {
            return sb.toString();
        } else {
            return null;
        }
    }

    public Map<String, String> getOverrideMapFor(Environment environment, HmsMirrorConfig config) {

        Map<String, String> rtn = new TreeMap<String, String>();
        for (Map.Entry<String, Map<SideType, String>> entry : config.getOptimization().getOverrides().getProperties().entrySet()) {
            switch (environment) {
                case LEFT:
                    if (entry.getValue().containsKey(SideType.LEFT)) {
                        rtn.put(entry.getKey(), entry.getValue().get(SideType.LEFT));
                    }
                    break;
                case RIGHT:
                    if (entry.getValue().containsKey(SideType.RIGHT)) {
                        rtn.put(entry.getKey(), entry.getValue().get(SideType.RIGHT));
                    }
                    break;
            }
            if (entry.getValue().containsKey(SideType.BOTH)) {
                rtn.put(entry.getKey(), entry.getValue().get(SideType.BOTH));
            }
        }

        return rtn;
    }

}

