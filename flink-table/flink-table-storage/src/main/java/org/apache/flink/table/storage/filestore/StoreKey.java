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

package org.apache.flink.table.storage.filestore;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * An internal key, including user key, sequence number and value kind. Sequence number indicates
 * the sequence of record. The large number will overwrite or merge the small number.
 */
public class StoreKey {

    private final BinaryRowData userKey;

    private final long sequenceNumber;

    private final ValueKind valueKind;

    public StoreKey(BinaryRowData userKey, long sequenceNumber, ValueKind valueKind) {
        this.userKey = userKey;
        this.sequenceNumber = sequenceNumber;
        this.valueKind = valueKind;
    }

    public RowData userKey() {
        return userKey;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public ValueKind valueKind() {
        return valueKind;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StoreKey storeKey = (StoreKey) o;
        return sequenceNumber == storeKey.sequenceNumber
                && Objects.equals(userKey, storeKey.userKey)
                && valueKind == storeKey.valueKind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(userKey, sequenceNumber, valueKind);
    }

    /** Schema of {@link StoreKey}. */
    public static RowType schema(RowType keyType) {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("_USER_KEY", keyType));
        fields.add(new RowType.RowField("_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new RowType.RowField("_VALUE_KIND", new TinyIntType(false)));
        return new RowType(fields);
    }

    /** Compare {@link StoreKey}. */
    public static int compare(
            Comparator<RowData> userKeyComparator, StoreKey left, StoreKey right) {
        return compare(
                userKeyComparator,
                left.userKey,
                left.sequenceNumber,
                right.userKey,
                right.sequenceNumber);
    }

    /** Compare user key and sequence number. */
    public static int compare(
            Comparator<RowData> userKeyComparator,
            RowData useKey1,
            long sequenceNumber1,
            RowData useKey2,
            long sequenceNumber2) {
        int result = userKeyComparator.compare(useKey1, useKey2);
        if (result != 0) {
            return result;
        }

        return Long.compare(sequenceNumber1, sequenceNumber2);
    }
}
