package com.streever.hadoop.hms.mirror.feature;

import java.util.List;

public interface Feature {
    List<String> fixSchema(List<String> schema);
}
