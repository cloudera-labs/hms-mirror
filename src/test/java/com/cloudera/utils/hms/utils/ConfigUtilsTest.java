package com.cloudera.utils.hms.utils;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.SideType;
import com.cloudera.utils.hms.util.ConfigUtils;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

import static javax.swing.UIManager.put;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigUtilsTest {

    @Test
    public void testGetPropertyOverridesFor() {
        // Test case 1
        // Given
        Environment environment = Environment.LEFT;
        HmsMirrorConfig config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("tez.queue.name", new HashMap<SideType, String>() {{
            put(SideType.LEFT, "queue1");
            put(SideType.RIGHT, "queue2");
        }});
        config.getOptimization().getOverrides().getProperties().put("something.null", new HashMap<SideType, String>() {{
            put(SideType.LEFT, null);
            put(SideType.RIGHT, null);
        }});
        List<String> result = ConfigUtils.getPropertyOverridesFor(environment, config);

        //        assertEquals("SET tez.queue.name=queue1", result);

        // Test case 2
        // Given
        environment = Environment.RIGHT;
        config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("mapred.job.queue.name", new HashMap<SideType, String>() {{
            put(SideType.LEFT, "queue1");
            put(SideType.RIGHT, "queue2");
        }});
        // When
        result = ConfigUtils.getPropertyOverridesFor(environment, config);
        // Then
//        assertEquals("SET mapred.job.queue.name=queue2", result);

        // Test case 3
        // Given
        environment = Environment.LEFT;
        config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("tez.queue.name", new HashMap<SideType, String>() {{
            put(SideType.BOTH, "queue1");
        }});
        // When
        result = ConfigUtils.getPropertyOverridesFor(environment, config);
        // Then
//        assertEquals("SET tez.queue.name=queue1", result);

        // Test case 4
        // Given
        environment = Environment.RIGHT;
        config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("mapred.job.queue.name", new HashMap<SideType, String>() {{
            put(SideType.BOTH, "queue1");
        }});
        config.getOptimization().getOverrides().getProperties().put("something.something", new HashMap<SideType, String>() {{
            put(SideType.BOTH, "BI");
        }});
        config.getOptimization().getOverrides().getProperties().put("another.setting", new HashMap<SideType, String>() {{
            put(SideType.RIGHT, "check");
        }});
        // When
        result = ConfigUtils.getPropertyOverridesFor(environment, config);
        // Then
//        assertEquals("SET mapred.job.queue.name=queue1", result);

        // Test case 5
        // Given
//        environment = Environment.LEFT;
//        config = new HmsMirrorConfig();
//        config.getOptimization().getOverrides().getProperties().put("tez.queue

    }

    @Test
    public void testGetQueuePropertyOverride() {
        // Test case 1
        // Given
        Environment environment = Environment.LEFT;
        HmsMirrorConfig config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("tez.queue.name", new HashMap<SideType, String>() {{
            put(SideType.LEFT, "queue1");
            put(SideType.RIGHT, "queue2");
        }});
        // When
        String result = ConfigUtils.getQueuePropertyOverride(environment, config);
        // Then
        assertEquals("SET tez.queue.name=queue1", result);

        // Test case 2
        // Given
        environment = Environment.RIGHT;
        config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("mapred.job.queue.name", new HashMap<SideType, String>() {{
            put(SideType.LEFT, "queue1");
            put(SideType.RIGHT, "queue2");
        }});
        // When
        result = ConfigUtils.getQueuePropertyOverride(environment, config);
        // Then
        assertEquals("SET mapred.job.queue.name=queue2", result);

        // Test case 3
        // Given
        environment = Environment.LEFT;
        config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("tez.queue.name", new HashMap<SideType, String>() {{
            put(SideType.BOTH, "queue1");
        }});
        // When
        result = ConfigUtils.getQueuePropertyOverride(environment, config);
        // Then
        assertEquals("SET tez.queue.name=queue1", result);

        // Test case 4
        // Given
        environment = Environment.RIGHT;
        config = new HmsMirrorConfig();
        config.getOptimization().getOverrides().getProperties().put("mapred.job.queue.name", new HashMap<SideType, String>() {{
            put(SideType.BOTH, "queue1");
        }});
        config.getOptimization().getOverrides().getProperties().put("something.something", new HashMap<SideType, String>() {{
            put(SideType.BOTH, "BI");
        }});
        // When
        result = ConfigUtils.getQueuePropertyOverride(environment, config);
        // Then
        assertEquals("SET mapred.job.queue.name=queue1", result);

        // Test case 5
        // Given
//        environment = Environment.LEFT;
//        config = new HmsMirrorConfig();
//        config.getOptimization().getOverrides().getProperties().put("tez.queue

    }
}
