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

package org.apache.flink.table.storage.filestore.utils;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.filestore.KeyValue;
import org.apache.flink.table.storage.filestore.ValueKind;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;

import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/** A test {@link RecordReader}. */
public class TestRecordReader implements RecordReader<KeyValue> {

    public static final Comparator<RowData> USER_KEY_COMPARATOR =
            Comparator.comparingInt(o -> o.getInt(0));

    public static final RowDataSerializer USER_KEY_SERIALIZER =
            new RowDataSerializer(new IntType());

    public static final RowDataSerializer VALUE_SERIALIZER =
            new RowDataSerializer(new BigIntType());

    private final LinkedList<Tuple3<Integer, ValueKind, Long>> records;
    private final int batchSize;
    private final KeyValue previous = new KeyValue();
    private final KeyValue current = new KeyValue();

    private boolean closed = false;

    public TestRecordReader(List<Tuple3<Integer, ValueKind, Long>> records, int batchSize) {
        this.records = new LinkedList<>(records);
        this.batchSize = batchSize;
    }

    @Nullable
    @Override
    public RecordIterator<KeyValue> readBatch() {
        LinkedList<Tuple3<Integer, ValueKind, Long>> batch = new LinkedList<>();
        for (int i = 0; i < batchSize; i++) {
            Tuple3<Integer, ValueKind, Long> value = records.poll();
            if (value == null) {
                break;
            } else {
                batch.add(value);
            }
        }

        if (batch.size() > 0) {
            return new TestRecordIterator(batch, previous, current);
        }

        return null;
    }

    @Override
    public void close() {
        this.closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    private static class TestRecordIterator implements RecordIterator<KeyValue> {

        private final LinkedList<Tuple3<Integer, ValueKind, Long>> records;

        private KeyValue previous;
        private KeyValue current;

        private TestRecordIterator(
                LinkedList<Tuple3<Integer, ValueKind, Long>> records,
                KeyValue reusePrevious,
                KeyValue reuseCurrent) {
            this.records = records;
            this.previous = reusePrevious;
            this.current = reuseCurrent;
        }

        private void switchCurrent() {
            KeyValue tmp = previous;
            previous = current;
            current = tmp;
        }

        @Override
        public boolean advanceNext() {
            switchCurrent();
            Tuple3<Integer, ValueKind, Long> value = records.poll();
            if (value == null) {
                return false;
            }

            current.replace(GenericRowData.of(value.f0), 0, value.f1, GenericRowData.of(value.f2));
            return true;
        }

        @Override
        public boolean singleInstance() {
            return false;
        }

        @Override
        public KeyValue current() {
            return current;
        }

        @Override
        public void releaseBatch() {}
    }
}
