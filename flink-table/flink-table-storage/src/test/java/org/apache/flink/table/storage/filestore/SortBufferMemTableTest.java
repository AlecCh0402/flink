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
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.filestore.utils.RecordIterator;
import org.apache.flink.table.storage.filestore.utils.RecordReader;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Assert;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/** Test for {@link SortBufferMemTable}. */
public class SortBufferMemTableTest {

    public static final RowDataSerializer INT_SERIALIZER = new RowDataSerializer(new IntType());

    private final SortBufferMemTable table =
            new SortBufferMemTable(
                    new RowType(
                            Collections.singletonList(new RowType.RowField("key", new IntType()))),
                    new RowType(
                            Collections.singletonList(
                                    new RowType.RowField("value", new IntType()))),
                    SortBufferMemTable.PAGE_SIZE * 3L);

    @Test
    public void testAndClear() throws IOException {
        innerTestRandom(100);
        table.clear();
        innerTestRandom(200);
    }

    @Test
    public void testOverflow() throws IOException {
        innerTestRandom(Integer.MAX_VALUE);
        table.clear();
        innerTestRandom(Integer.MAX_VALUE);
    }

    private void innerTestRandom(int numRecords) throws IOException {
        int bound = 100;
        Random rnd = new Random();
        List<KeyValue> inputs = new ArrayList<>();
        Set<Integer> usedSequenceNumber = new HashSet<>();
        for (int i = 0; i < numRecords; i++) {
            int value = rnd.nextInt(bound);
            int sequenceNumber;
            while (true) {
                sequenceNumber = rnd.nextInt();
                if (!usedSequenceNumber.contains(sequenceNumber)) {
                    usedSequenceNumber.add(sequenceNumber);
                    break;
                }
            }
            KeyValue keyValue =
                    new KeyValue()
                            .replace(
                                    GenericRowData.of(value),
                                    sequenceNumber,
                                    rnd.nextBoolean() ? ValueKind.ADD : ValueKind.DELETE,
                                    GenericRowData.of(value));
            boolean success =
                    table.put(
                            keyValue.sequenceNumber(),
                            keyValue.valueKind(),
                            keyValue.key(),
                            keyValue.value());
            if (!success) {
                if (numRecords == Integer.MAX_VALUE) {
                    break;
                } else {
                    throw new EOFException();
                }
            }
            inputs.add(keyValue);
        }

        Assert.assertEquals(inputs.size(), table.size());

        RecordReader<KeyValue> reader = table.createOrderedReader();

        RecordIterator<KeyValue> iterator = reader.readBatch();
        List<KeyValue> results = new ArrayList<>();
        Assert.assertNotNull(iterator);
        while (iterator.advanceNext()) {
            KeyValue current = iterator.current();
            if (results.size() > 0) {
                Assert.assertTrue(isEquals(results.get(results.size() - 1), iterator.previous()));
            }
            results.add(current.copy(INT_SERIALIZER, INT_SERIALIZER));
        }
        Assert.assertNull(reader.readBatch());

        inputs.sort(
                Comparator.<KeyValue>comparingInt(o -> o.key().getInt(0))
                        .thenComparingLong(KeyValue::sequenceNumber));

        Assert.assertEquals(results.size(), inputs.size());
        for (int i = 0; i < results.size(); i++) {
            Assert.assertTrue(isEquals(inputs.get(i), results.get(i)));
        }
    }

    private boolean isEquals(KeyValue kv1, KeyValue kv2) {
        return kv1.sequenceNumber() == kv2.sequenceNumber()
                && kv1.valueKind() == kv2.valueKind()
                && kv1.key().getInt(0) == kv2.key().getInt(0)
                && kv1.value().getInt(0) == kv2.value().getInt(0);
    }
}
