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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.filestore.utils.OffsetRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;

import java.util.ArrayList;
import java.util.List;

/**
 * A key value, including user key, sequence number, value kind and value. This object can be
 * reused.
 */
public class KeyValue {

    private RowData key;
    private long sequenceNumber;
    private ValueKind valueKind;
    private RowData value;

    public KeyValue replace(RowData key, long sequenceNumber, ValueKind valueKind, RowData value) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
        this.valueKind = valueKind;
        this.value = value;
        return this;
    }

    public KeyValue setValue(RowData value) {
        this.value = value;
        return this;
    }

    public KeyValue setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
        return this;
    }

    public KeyValue setValueKind(ValueKind valueKind) {
        this.valueKind = valueKind;
        return this;
    }

    public KeyValue copy(RowDataSerializer userKeySerializer, RowDataSerializer valueSerializer) {
        return new KeyValue()
                .replace(
                        userKeySerializer.copy(key),
                        sequenceNumber,
                        valueKind,
                        valueSerializer.copy(value));
    }

    public RowData key() {
        return key;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public ValueKind valueKind() {
        return valueKind;
    }

    public RowData value() {
        return value;
    }

    public static RowType schema(RowType keyType, RowType valueType) {
        List<RowType.RowField> fields = new ArrayList<>(keyType.getFields());
        fields.add(new RowType.RowField("_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new RowType.RowField("_VALUE_KIND", new TinyIntType(false)));
        fields.addAll(valueType.getFields());
        return new RowType(fields);
    }

    /** A converter to convert {@link KeyValue} to {@link RowData}. The return row is reused. */
    public static class ToRowConverter {

        private final GenericRowData meta = new GenericRowData(2);
        private final JoinedRowData keyWithMeta = new JoinedRowData();
        private final JoinedRowData row = new JoinedRowData();

        public RowData replace(KeyValue keyValue) {
            return replace(
                    keyValue.key, keyValue.sequenceNumber, keyValue.valueKind, keyValue.value);
        }

        public RowData replace(
                RowData key, long sequenceNumber, ValueKind valueKind, RowData value) {
            meta.setField(0, sequenceNumber);
            meta.setField(1, valueKind.toByteValue());
            return row.replace(keyWithMeta.replace(key, meta), value);
        }
    }

    /**
     * A converter to convert {@link RowData} to {@link KeyValue}. The return key value is reused.
     */
    public static class FromRowConverter<T extends RowData> {

        private final int keyArity;
        private final KeyValue keyValue;
        private final OffsetRowData key;
        private final OffsetRowData value;

        private T row;

        public FromRowConverter(int keyArity, int valueArity) {
            this.keyArity = keyArity;
            this.key = new OffsetRowData(keyArity, 0);
            this.value = new OffsetRowData(valueArity, keyArity + 2);
            this.keyValue = new KeyValue().replace(key, -1, null, value);
        }

        public KeyValue replace(T row) {
            this.row = row;
            this.key.replace(row);
            this.value.replace(row);
            this.keyValue.setSequenceNumber(row.getLong(keyArity));
            this.keyValue.setValueKind(ValueKind.fromByteValue(row.getByte(keyArity + 1)));
            return keyValue;
        }

        public KeyValue record() {
            return keyValue;
        }

        public T row() {
            return row;
        }
    }
}
