package com.cloudera.utils.hadoop.hms.mirror;

import org.jetbrains.annotations.NotNull;

import java.util.*;

public class EnvironmentMap {

    public class TranslationLevel {
        int level;
        String original, target;

        public TranslationLevel(String original, String target, int level) {
            this.original = original;
            this.target = target;
            this.level = level;
        }

        public String getAdjustedOriginal() {
            return Translator.reduceUrlBy(original, level);
        }

        public String getAdjustedTarget() {
            return Translator.reduceUrlBy(target, level);
        }
    }

//    private final String database;
    private final Map<Environment, Set<TranslationLevel>> environmentMap = new TreeMap<>();
//    private final Set<TranslationLevel> environmentSet = new HashSet<>();

//    public EnvironmentMap(String database) {
//        this.database = database;
//    }

    public void addTranslationLocation(Environment environment, String original, String target, int level) {
        Set<TranslationLevel> dbTranslationSet = environmentMap.computeIfAbsent(environment, k -> new HashSet<TranslationLevel>());
        dbTranslationSet.add(new TranslationLevel(original, target, level));
    }

    public Set<TranslationLevel> getTranslationSet(Environment environment) {
        Set<TranslationLevel> dbTranslationSet = environmentMap.computeIfAbsent(environment, k -> new HashSet<TranslationLevel>());
        return dbTranslationSet;
    }

//    public synchronized Map<String, String> getLocationMap1(Environment environment) {
//        Map<String, String> rtn = environmentMap.get(environment);
//        if (rtn == null) {
//            rtn = new TreeMap<String, String>();
//            environmentMap.put(environment, rtn);
//        }
//        return rtn;
//    }
}
