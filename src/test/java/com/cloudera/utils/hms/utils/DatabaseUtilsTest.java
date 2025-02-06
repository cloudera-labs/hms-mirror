package com.cloudera.utils.hms.utils;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.service.DatabaseService;
import com.cloudera.utils.hms.util.DatabaseUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DatabaseUtilsTest {

    private static final Map<String, String> parameters = new HashMap<>();

    @Before
    public void setup() {
        parameters.put("dont.move.me", "1");
        parameters.put("repl.test1", "2");
        parameters.put("repl.test2", "3");
        parameters.put("skip3", "4");

    }

    @Test
    public void dbPropertiesSkipTest_01() {
        HmsMirrorConfig config = new HmsMirrorConfig();
        config.getFilter().addDbPropertySkipItem("dont\\.move\\.me");
        config.getFilter().addDbPropertySkipItem("repl\\.test.*");
        Map<String, String> results = DatabaseUtils.getParameters(parameters, DatabaseService.skipList, config.getFilter().getDbPropertySkipListPattern());
        // We should clear out all but the 'skip3' entry
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("4", results.get("skip3"));
    }

    @Test
    public void dbPropertiesSkipTest_02() {
        HmsMirrorConfig config = new HmsMirrorConfig();
        config.getFilter().getDbPropertySkipList().add("dont\\.move\\.me");
        config.getFilter().getDbPropertySkipList().add("repl\\.test.*");
        Map<String, String> results = DatabaseUtils.getParameters(parameters, DatabaseService.skipList, config.getFilter().getDbPropertySkipListPattern());
        // We should clear out all but the 'skip3' entry
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("4", results.get("skip3"));
    }

}
