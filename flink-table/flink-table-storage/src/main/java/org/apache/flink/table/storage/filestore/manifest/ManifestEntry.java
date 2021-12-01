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

package org.apache.flink.table.storage.filestore.manifest;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.storage.filestore.ValueKind;
import org.apache.flink.table.storage.filestore.lsm.sst.SstFileMeta;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Entry of a manifest file, representing an addition / deletion of a SST file. */
public class ManifestEntry {

    private final ValueKind kind;
    private final BinaryRowData partition;
    private final int bucket;
    private final int totalBuckets;
    private final SstFileMeta file;

    public ManifestEntry(
            ValueKind kind,
            BinaryRowData partition,
            int bucket,
            int totalBuckets,
            SstFileMeta file) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.file = file;
    }

    public ValueKind kind() {
        return kind;
    }

    public BinaryRowData partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public SstFileMeta file() {
        return file;
    }

    public static RowType schema(RowType partitionType, RowType rowType, RowType keyType) {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("_KIND", new TinyIntType(false)));
        fields.add(new RowType.RowField("_PARTITION", partitionType));
        fields.add(new RowType.RowField("_BUCKET", new IntType(false)));
        fields.add(new RowType.RowField("_TOTAL_BUCKETS", new IntType(false)));
        fields.add(new RowType.RowField("_FILE", SstFileMeta.schema(rowType, keyType)));
        return new RowType(fields);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestEntry)) {
            return false;
        }
        ManifestEntry that = (ManifestEntry) o;
        return Objects.equals(kind, that.kind)
                && Objects.equals(partition, that.partition)
                && bucket == that.bucket
                && totalBuckets == that.totalBuckets
                && Objects.equals(file, that.file);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, partition, bucket, totalBuckets, file);
    }

    @Override
    public String toString() {
        return String.format("{%s, %s, %d, %d, %s}", kind, partition, bucket, totalBuckets, file);
    }
}
