/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hms.mirror.domain.support;

import lombok.Getter;

@Getter
public class OperationStatistics implements Cloneable{

    private final OperationStatistic counts = new OperationStatistic();
    private final OperationStatistic skipped = new OperationStatistic();
    private final OperationStatistic issues = new OperationStatistic();
    private final OperationStatistic failures = new OperationStatistic();
    private final OperationStatistic successes = new OperationStatistic();

    @Override
    public OperationStatistics clone() {
        try {
            OperationStatistics clone = (OperationStatistics) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    public void reset() {
        counts.reset();
        issues.reset();
        failures.reset();
        successes.reset();
    }

}
