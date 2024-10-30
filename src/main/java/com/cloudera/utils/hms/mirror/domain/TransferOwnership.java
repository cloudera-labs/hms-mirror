package com.cloudera.utils.hms.mirror.domain;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TransferOwnership {
    private boolean database = Boolean.FALSE;
    private boolean table = Boolean.FALSE;
}
