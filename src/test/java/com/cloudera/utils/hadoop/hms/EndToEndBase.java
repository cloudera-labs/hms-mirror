package com.cloudera.utils.hadoop.hms;

public class EndToEndBase {
    protected static String homedir = System.getProperty("user.home");
    protected static String separator = System.getProperty("file.separator");

    private String outputDirBase = null;

    protected String getOutputDirBase() {
        if (outputDirBase == null) {
            outputDirBase = homedir + separator + "hms-mirror-reports" + separator +
                    DataState.getInstance().getUnique() + separator +
                    getClass().getSimpleName() + separator;
        }
        return outputDirBase;
    }
}
