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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.filestore.KeyValue;
import org.apache.flink.table.storage.filestore.ValueKind;
import org.apache.flink.table.storage.filestore.combine.CombinePolicy;
import org.apache.flink.table.storage.filestore.combine.SortMergeReader;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;

import org.junit.Assert;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.storage.filestore.ValueKind.ADD;

/** A test {@link RecordReader}. */
public class TestRecordReader implements RecordReader<KeyValue> {

    public static final Comparator<RowData> USER_KEY_COMPARATOR =
            Comparator.comparingInt(o -> o.getInt(0));

    public static final RowDataSerializer USER_KEY_SERIALIZER =
            new RowDataSerializer(new IntType());

    public static final RowDataSerializer VALUE_SERIALIZER =
            new RowDataSerializer(new BigIntType());

    private final LinkedList<TestKeyValue> records;
    private final int batchSize;
    private final KeyValue previous = new KeyValue();
    private final KeyValue current = new KeyValue();

    private boolean closed = false;

    public TestRecordReader(List<TestKeyValue> records, int batchSize) {
        this.records = new LinkedList<>(records);
        this.batchSize = batchSize;
    }

    @Nullable
    @Override
    public RecordIterator<KeyValue> readBatch() {
        LinkedList<TestKeyValue> batch = new LinkedList<>();
        for (int i = 0; i < batchSize; i++) {
            TestKeyValue value = records.poll();
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

        private final LinkedList<TestKeyValue> records;

        private KeyValue previous;
        private KeyValue current;

        private TestRecordIterator(
                LinkedList<TestKeyValue> records, KeyValue reusePrevious, KeyValue reuseCurrent) {
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
            TestKeyValue value = records.poll();
            if (value == null) {
                return false;
            }

            current.replace(
                    GenericRowData.of(value.key),
                    value.sequenceNumber,
                    value.valueKind,
                    GenericRowData.of(value.value));
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

    /** A test POJO. */
    public static class TestKeyValue {

        private final Integer key;
        private final ValueKind valueKind;
        private final long sequenceNumber;
        private final Long value;

        public TestKeyValue(Integer key, ValueKind valueKind) {
            this(key, valueKind, 0L, key.longValue());
        }

        public TestKeyValue(Integer key, ValueKind valueKind, Long value) {
            this(key, valueKind, 0L, value);
        }

        public TestKeyValue(Integer key, long sequenceNumber) {
            this(key, ADD, sequenceNumber, 0L);
        }

        public TestKeyValue(Integer key, ValueKind valueKind, long sequenceNumber, Long value) {
            this.key = key;
            this.valueKind = valueKind;
            this.sequenceNumber = sequenceNumber;
            this.value = value;
        }

        public Integer getKey() {
            return this.key;
        }

        public long getSequenceNumber() {
            return this.sequenceNumber;
        }

        public static TestKeyValue from(RecordIterator<KeyValue> iterator) {
            return new TestKeyValue(
                    iterator.current().key().getInt(0),
                    iterator.current().valueKind(),
                    iterator.current().sequenceNumber(),
                    iterator.current().value().getLong(0));
        }

        public static TestKeyValue from(
                CombinePolicy combinePolicy, RecordIterator<KeyValue> iterator) {
            return combinePolicy == CombinePolicy.DEDUPLICATE
                    ? new TestKeyValue(
                            iterator.current().key().getInt(0), iterator.current().valueKind())
                    : new TestKeyValue(
                            iterator.current().key().getInt(0),
                            iterator.current().valueKind(),
                            iterator.current().value().getLong(0));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof TestKeyValue)) {
                return false;
            }

            TestKeyValue that = (TestKeyValue) o;

            return sequenceNumber == that.sequenceNumber
                    && Objects.equals(key, that.key)
                    && valueKind == that.valueKind
                    && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, valueKind, sequenceNumber, value);
        }

        @Override
        public String toString() {
            return "TestKeyValue{"
                    + "key="
                    + key
                    + ", valueKind="
                    + valueKind
                    + ", sequenceNumber="
                    + sequenceNumber
                    + ", value="
                    + value
                    + '}';
        }
    }

    public static void testCombine(
            CombinePolicy combinePolicy,
            List<TestKeyValue> inputs,
            List<TestKeyValue> expected,
            int batchSize)
            throws IOException {
        TestRecordReader reader = new TestRecordReader(inputs, batchSize);
        RecordReader<KeyValue> combineReader =
                combinePolicy.combine(
                        reader, USER_KEY_COMPARATOR, USER_KEY_SERIALIZER, VALUE_SERIALIZER);
        List<TestKeyValue> values = new ArrayList<>();
        RecordIterator<KeyValue> batch;
        while ((batch = combineReader.readBatch()) != null) {
            while (batch.advanceNext()) {
                values.add(TestKeyValue.from(combinePolicy, batch));
            }
        }
        combineReader.close();
        Assert.assertTrue(reader.isClosed());
        Assert.assertEquals(expected, values);
    }

    public static void testSortMerge(List<List<TestKeyValue>> inputs, int batchSize)
            throws IOException {
        List<RecordReader<KeyValue>> readers = new ArrayList<>();
        for (List<TestKeyValue> input : inputs) {
            readers.add(new TestRecordReader(input, batchSize));
        }
        RecordReader<KeyValue> sortMergeReader = new SortMergeReader(readers, USER_KEY_COMPARATOR);
        List<TestKeyValue> actual = new ArrayList<>();
        List<KeyValue> reused = new ArrayList<>();
        RecordIterator<KeyValue> batch;
        KeyValue previous = null;
        while ((batch = sortMergeReader.readBatch()) != null) {
            while (batch.advanceNext()) {
                if (previous != null) {
                    Assert.assertEquals(reused.get(reused.size() - 1), previous);
                }
                previous = batch.current();
                reused.add(previous);
                actual.add(TestKeyValue.from(batch));
            }
        }
        sortMergeReader.close();
        for (RecordReader<KeyValue> reader : readers) {
            Assert.assertTrue(((TestRecordReader) reader).isClosed());
        }
        List<TestKeyValue> expected = generateExpected(inputs, null);
        Assert.assertEquals(expected, actual);
    }

    public static void testSortMergeWithCombiner(
            List<List<TestKeyValue>> inputs, CombinePolicy combinePolicy, int batchSize)
            throws IOException {
        List<RecordReader<KeyValue>> readers = new ArrayList<>();
        for (List<TestKeyValue> input : inputs) {
            readers.add(new TestRecordReader(input, batchSize));
        }
        RecordReader<KeyValue> sortMergeReader = new SortMergeReader(readers, USER_KEY_COMPARATOR);
        RecordReader<KeyValue> combineReader =
                combinePolicy.combine(
                        sortMergeReader,
                        USER_KEY_COMPARATOR,
                        USER_KEY_SERIALIZER,
                        VALUE_SERIALIZER);
        List<TestKeyValue> actual = new ArrayList<>();
        RecordIterator<KeyValue> batch;
        while ((batch = combineReader.readBatch()) != null) {
            while (batch.advanceNext()) {
                actual.add(TestKeyValue.from(batch));
            }
        }
        sortMergeReader.close();
        for (RecordReader<KeyValue> reader : readers) {
            Assert.assertTrue(((TestRecordReader) reader).isClosed());
        }
        List<TestKeyValue> expected = generateExpected(inputs, combinePolicy);
        Assert.assertEquals(expected, actual);
    }

    private static List<TestKeyValue> generateExpected(
            List<List<TestKeyValue>> inputs, CombinePolicy combinePolicy) {
        List<TestKeyValue> expected =
                inputs.stream()
                        .flatMap(Collection::stream)
                        .sorted(
                                Comparator.comparing(TestRecordReader.TestKeyValue::getKey)
                                        .thenComparingLong(
                                                TestRecordReader.TestKeyValue::getSequenceNumber))
                        .collect(Collectors.toList());
        if (combinePolicy == null) {
            return expected;
        }
        return expected.stream()
                .map(Collections::singletonList)
                .reduce(
                        new ArrayList<>(),
                        (combined, current) ->
                                accumulate(
                                        combinePolicy,
                                        (ArrayList<TestKeyValue>) combined,
                                        current.get(0)));
    }

    private static ArrayList<TestKeyValue> accumulate(
            CombinePolicy combinePolicy, ArrayList<TestKeyValue> combined, TestKeyValue current) {
        if (combined.isEmpty()) {
            combined.add(current);
        } else {
            TestKeyValue previous = combined.get(combined.size() - 1);
            if (!previous.key.equals(current.key)) {
                combined.add(current);
            } else {
                combined.remove(previous);
                combined.add(
                        combinePolicy == CombinePolicy.DEDUPLICATE
                                ? current
                                : new TestKeyValue(
                                        current.key,
                                        current.valueKind,
                                        current.sequenceNumber,
                                        current.value + previous.value));
            }
        }
        return combined;
    }
}
