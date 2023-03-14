package com.cloudera.utils.hadoop.hms.mirror;

import java.util.Properties;

public class Overrides {
    public enum Side { BOTH, LEFT, RIGHT };
    private Properties left = null;
    private Properties right = null;

    public Properties getLeft() {
        if (left == null)
            left = new Properties();
        return left;
    }

    public void setLeft(Properties left) {
        this.left = left;
    }

    public Properties getRight() {
        if (right == null)
            right = new Properties();
        return right;
    }

    public void setRight(Properties right) {
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
                                getLeft().setProperty(keyValue[0], keyValue[1]);
                                getRight().setProperty(keyValue[0], keyValue[1]);
                                break;
                            case LEFT:
                                getLeft().setProperty(keyValue[0], keyValue[1]);
                                break;
                            case RIGHT:
                                getRight().setProperty(keyValue[0], keyValue[1]);
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
