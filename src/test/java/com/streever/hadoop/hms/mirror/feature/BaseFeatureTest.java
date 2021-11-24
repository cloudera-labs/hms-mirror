package com.streever.hadoop.hms.mirror.feature;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BaseFeatureTest {
    public List<String> toList(String[] array) {
        List<String> rtn = new ArrayList<String>();
        Collections.addAll(rtn, array);
        return rtn;
    }

}
