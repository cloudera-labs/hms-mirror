package com.cloudera.utils.hms.mirror;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.text.MessageFormat;

@Slf4j
public class TestSessionVars {

    @Test
    public void testSetSessionVars() {
        String result = MessageFormat.
                format(SessionVars.SET_SESSION_VALUE, SessionVars.SORT_DYNAMIC_PARTITION_THRESHOLD, "-1");
        assertEquals("SET hive.optimize.sort.dynamic.partition.threshold=-1", result);
    }
}
