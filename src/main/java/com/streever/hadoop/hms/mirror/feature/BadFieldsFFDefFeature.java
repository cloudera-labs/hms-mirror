package com.streever.hadoop.hms.mirror.feature;

import com.streever.hadoop.hms.mirror.EnvironmentTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.regex.Pattern;

public class BadFieldsFFDefFeature extends BaseFeature implements Feature {
    private final String FTB = "FIELDS TERMINATED BY";
    private final Pattern FIELDS_TERMINATED_BY = Pattern.compile(FTB + " '(.*)'");

    private static Logger LOG = LogManager.getLogger(BadFieldsFFDefFeature.class);

    public String getDescription() {
        return "Table schemas with a \\f definition in the FIELDS TERMINATED BY declaration will not be created correctly " +
                "in Hive.  We need to set the value to the character set value \\014 to successfully translate the schema.";
    }

    @Override
    public Boolean applicable(EnvironmentTable envTable) {
        return applicable(envTable.getDefinition());
    }

    @Override
    /*
    check if '\f' is used in FIELDS TERMINATED BY.
     */
    public Boolean applicable(List<String> schema) {
        Boolean rtn = Boolean.FALSE;

        String v = getGroupFor(FIELDS_TERMINATED_BY, schema);
        try {
            if (v != null && Character.compare('f', v.charAt(1)) == 0) {
                rtn = Boolean.TRUE;
            }
        } catch (Throwable t) {
            // skip
        }

        return rtn;
    }


    @Override
    /*
    replace '\f' with '\014' in FIELD TERMINATED BY because HIVE won't take this.
     */
    public EnvironmentTable fixSchema(EnvironmentTable envTable) {
        List<String> fixed = fixSchema(envTable.getDefinition());
        envTable.setDefinition(fixed);
        return envTable;
    }

    @Override
    /**
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '\f'

     The '\f' won't translate in schemas that are replayed.  It needs to be converted
     to '\014' and then hive will translate it to '\f'.
     */
    public List<String> fixSchema(List<String> schema) {
        List<String> rtn = addEscaped(schema);
        LOG.debug("Checking if table has Bad Fields definition");
        // find the index of the ROW_FORMAT_DELIMITED

        String v = getGroupFor(FIELDS_TERMINATED_BY, schema);


        Boolean badDef = Boolean.FALSE;
        try {
            if (v != null && Character.compare('f', v.charAt(1)) == 0) {
                badDef = Boolean.TRUE;
            }
        } catch (Throwable t) {
            // skip
        }

        if (badDef) {
            // Find location.
            int loc = -1;
            Boolean found = Boolean.FALSE;
            for (String line : rtn) {
                loc++;
                if (line.trim().startsWith(FTB)) {
                    found = Boolean.TRUE;
                    break;
                }
            }
            if (found) {
                rtn.set(loc, FTB + " '\\014'");
            }
        }

        return rtn;
    }

}
