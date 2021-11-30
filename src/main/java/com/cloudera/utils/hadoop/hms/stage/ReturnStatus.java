package com.cloudera.utils.hadoop.hms.stage;

public class ReturnStatus {
    public enum Status { SUCCESS, ERROR };
    private Status status = null;
//    private String message = null;
    private Throwable exception = null;

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.exception = exception;
    }
}
