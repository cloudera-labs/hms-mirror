package com.cloudera.utils.hadoop.hms.mirror;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public class Overrides {
    public enum Side { BOTH, LEFT, RIGHT };
    private Map<String, String> left = null;
    private Map<String, String> right = null;

    public Map<String, String> getLeft() {
        if (left == null)
            left = new TreeMap<String, String>();
        return left;
    }

    public void setLeft(Map<String, String> left) {
        this.left = left;
    }

    public Map<String, String> getRight() {
        if (right == null)
            right = new TreeMap<String, String>();
        return right;
    }

    public void setRight(Map<String, String> right) {
        this.right = right;
    }

    public Boolean setPropertyOverridesStr(String[] inPropsStr, Side side) {
        Boolean set = Boolean.TRUE;
        if (inPropsStr != null) {
            for (String property : inPropsStr) {
                try {
                    String[] keyValue = property.split("=");
                    if (keyValue.length == 2) {
                        switch (side) {
                            case BOTH:
                                getLeft().put(keyValue[0], keyValue[1]);
                                getRight().put(keyValue[0], keyValue[1]);
                                break;
                            case LEFT:
                                getLeft().put(keyValue[0], keyValue[1]);
                                break;
                            case RIGHT:
                                getRight().put(keyValue[0], keyValue[1]);
                                break;
                        }
                    }
                } catch (Throwable t) {
                    set = Boolean.FALSE;
                }
            }
        }
        return set;
    }

}
