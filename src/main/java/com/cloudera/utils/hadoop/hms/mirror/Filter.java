package com.cloudera.utils.hadoop.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.regex.Pattern;

public class Filter {
    @JsonIgnore
    private Pattern dbFilterPattern = null;
    @JsonIgnore // wip
    private String dbRegEx = null;
    @JsonIgnore
    private Pattern tblExcludeFilterPattern = null;
    @JsonIgnore
    private Pattern tblFilterPattern = null;
    private String tblExcludeRegEx = null;
    private String tblRegEx = null;
    private Long tblSizeLimit = null;
    private Integer tblPartitionLimit = null;

    public String getDbRegEx() {
        return dbRegEx;
    }

    public void setDbRegEx(String dbRegEx) {
        this.dbRegEx = dbRegEx;
        if (this.dbRegEx != null)
            dbFilterPattern = Pattern.compile(dbRegEx);
        else
            dbFilterPattern = null;

    }

    public Pattern getDbFilterPattern() {
        return dbFilterPattern;
    }

    public String getTblRegEx() {
        return tblRegEx;
    }

    public void setTblRegEx(String tblRegEx) {
        this.tblRegEx = tblRegEx;
        if (this.tblRegEx != null)
            tblFilterPattern = Pattern.compile(tblRegEx);
        else
            tblFilterPattern = null;
    }

    public String getTblExcludeRegEx() {
        return tblExcludeRegEx;
    }

    public void setTblExcludeRegEx(String tblExcludeRegEx) {
        this.tblExcludeRegEx = tblExcludeRegEx;
        if (this.tblExcludeRegEx != null)
            tblExcludeFilterPattern = Pattern.compile(tblExcludeRegEx);
        else
            tblExcludeFilterPattern = null;

    }
    public Pattern getTblFilterPattern() {
        return tblFilterPattern;
    }

    public Pattern getTblExcludeFilterPattern() {
        return tblExcludeFilterPattern;
    }

    public Long getTblSizeLimit() {
        return tblSizeLimit;
    }

    public void setTblSizeLimit(Long tblSizeLimit) {
        this.tblSizeLimit = tblSizeLimit;
    }

    public Integer getTblPartitionLimit() {
        return tblPartitionLimit;
    }

    public void setTblPartitionLimit(Integer tblPartitionLimit) {
        this.tblPartitionLimit = tblPartitionLimit;
    }
}
