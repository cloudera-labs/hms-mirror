package com.streever.hadoop.hms.mirror;

public class Marker {
    private String mark;
    private String description;
    private String action;

    public Marker(String mark, String description, Object action) {
        this.mark = mark;
        this.description = description;
        if (action == null)
            this.action = "";
        else
            this.action = action.toString();
    }

    public String getMark() {
        return mark;
    }

    public String getDescription() {
        return description;
    }

    public String getAction() {
        return action;
    }
}
