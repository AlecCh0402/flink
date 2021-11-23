/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.storage.filestore.lsm;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.storage.filestore.lsm.sst.SstFileMeta;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link SortedRun} is a list of files sorted by their keys. The key intervals [minKey, maxKey]
 * of these files do not overlap.
 */
public class SortedRun {

    private final List<SstFileMeta> files;

    public SortedRun(List<SstFileMeta> files) {
        this.files = files;
    }

    public List<SstFileMeta> files() {
        return files;
    }

    @VisibleForTesting
    public void validate(RecordComparator comparator) {
        for (int i = 1; i < files.size(); i++) {
            Preconditions.checkState(
                    comparator.compare(files.get(i).minKey(), files.get(i - 1).maxKey()) > 0,
                    "SortedRun is not sorted and may contain overlapping key intervals. This is a bug.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SortedRun)) {
            return false;
        }
        SortedRun that = (SortedRun) o;
        return files.equals(that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(files);
    }

    @Override
    public String toString() {
        return "["
                + files.stream().map(SstFileMeta::toString).collect(Collectors.joining(", "))
                + "]";
    }
}
