package com.streever.hadoop.hms.mirror.feature;

import com.streever.hadoop.hms.mirror.EnvironmentTable;

import java.util.List;

public interface Feature {
    List<String> fixSchema(List<String> schema);
    EnvironmentTable fixSchema(EnvironmentTable envTable);
}
