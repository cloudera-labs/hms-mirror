package com.streever.hadoop.hms.mirror.feature;


import org.apache.commons.text.StringEscapeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BaseFeature {

    protected Boolean contains(String search, List<String> definition) {
        Boolean rtn = Boolean.FALSE;
        for (String line : definition) {
            if (line.trim().toUpperCase(Locale.ROOT).startsWith(search.toUpperCase(Locale.ROOT))) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    protected String getGroupFor(Pattern pattern, List<String> definition) {
        String rtn = null;
        for (String line : definition) {
            String adjLine = StringEscapeUtils.escapeJava(line);
            Matcher m = pattern.matcher(adjLine);
            if (m.find()) {
                rtn = m.group(1);
            }
        }
        return rtn;
    }

    protected int indexOf(List<String> definition, String condition) {
        int rtn = -1;
        int loc = 0;
        for (String line : definition) {
            if (line.trim().toUpperCase(Locale.ROOT).startsWith(condition.toUpperCase(Locale.ROOT))) {
                rtn = loc;
                break;
            } else {
                loc++;
            }
        }
        return rtn;
    }

    protected Boolean removeRange(int from, int to, List<String> definition) {
        Boolean rtn = Boolean.FALSE;
        if (from < to) {
            for (int i = from; i < to; i++) {
                definition.remove(from);
            }
        }
        return rtn;
    }

    public List<String> addEscaped(List<String> definition) {
        List<String> escapedList = new ArrayList<String>();
        for (String line : definition) {
            if (line.contains("escape.delim")) {
                String escapedLine = StringEscapeUtils.escapeJava(line);
                escapedList.add(escapedLine);
            } else {
                escapedList.add(line);
            }
        }
        return escapedList;
    }
}
