package com.streever.hadoop.hms.mirror.feature;

import com.streever.hadoop.hms.mirror.EnvironmentTable;

import java.util.List;

public interface Feature {
    Boolean applicable(EnvironmentTable envTable);
    Boolean applicable(List<String> schema);
    List<String> fixSchema(List<String> schema);
    EnvironmentTable fixSchema(EnvironmentTable envTable);
    List<String> addEscaped(List<String> definition);
}
