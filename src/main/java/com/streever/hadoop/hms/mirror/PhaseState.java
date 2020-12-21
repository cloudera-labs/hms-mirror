package com.streever.hadoop.hms.mirror;

public enum PhaseState {
    INIT,STARTED,ERROR,
    SUCCESS,
    // This happens on RETRY only when it was previously SUCCESS.
    RETRY_SKIPPED_PAST_SUCCESS;
}
