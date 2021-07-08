package com.streever.hadoop.hms.mirror;

public class Pair {
    private String description;
    private String action;

    public Pair(String description, String action) {
        this.description = description;
        this.action = action;
    }

    public String getDescription() {
        return description;
    }

    public String getAction() {
        return action;
    }
}
