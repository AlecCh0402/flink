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

package org.apache.flink.table.storage.filestore.combine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.storage.filestore.KeyValue;
import org.apache.flink.table.storage.filestore.ValueKind;
import org.apache.flink.table.storage.filestore.utils.RecordIterator;
import org.apache.flink.table.storage.filestore.utils.RecordReader;
import org.apache.flink.table.storage.filestore.utils.TestRecordReader;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.storage.filestore.ValueKind.ADD;
import static org.apache.flink.table.storage.filestore.ValueKind.DELETE;
import static org.apache.flink.table.storage.filestore.utils.TestRecordReader.USER_KEY_COMPARATOR;
import static org.apache.flink.table.storage.filestore.utils.TestRecordReader.USER_KEY_SERIALIZER;
import static org.apache.flink.table.storage.filestore.utils.TestRecordReader.VALUE_SERIALIZER;

/** Test for {@link DeduplicateCombineReader}. */
public class DeduplicateCombineReaderTest {

    private static final int BATCH_SIZE = 3;

    @Test
    public void testEmpty() throws IOException {
        doTest(Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void testDelete() throws IOException {
        doTest(
                Arrays.asList(new Tuple2<>(1, ADD), new Tuple2<>(1, ADD), new Tuple2<>(1, DELETE)),
                Collections.singletonList(new Tuple2<>(1, DELETE)));
    }

    @Test
    public void testCrossBatch() throws IOException {
        doTest(
                Arrays.asList(
                        new Tuple2<>(1, ADD),
                        new Tuple2<>(1, ADD),
                        new Tuple2<>(1, ADD),
                        new Tuple2<>(1, DELETE)),
                Collections.singletonList(new Tuple2<>(1, DELETE)));
    }

    @Test
    public void testMultipleKeys() throws IOException {
        doTest(
                Arrays.asList(
                        new Tuple2<>(1, ADD),
                        new Tuple2<>(2, ADD),
                        new Tuple2<>(2, DELETE),
                        new Tuple2<>(3, ADD)),
                Arrays.asList(new Tuple2<>(1, ADD), new Tuple2<>(2, DELETE), new Tuple2<>(3, ADD)));
    }

    @Test
    public void testMultipleKeysCross() throws IOException {
        doTest(
                Arrays.asList(
                        new Tuple2<>(1, ADD),
                        new Tuple2<>(2, ADD),
                        new Tuple2<>(2, ADD),
                        new Tuple2<>(2, DELETE),
                        new Tuple2<>(3, ADD),
                        new Tuple2<>(4, ADD),
                        new Tuple2<>(5, ADD),
                        new Tuple2<>(6, ADD)),
                Arrays.asList(
                        new Tuple2<>(1, ADD),
                        new Tuple2<>(2, DELETE),
                        new Tuple2<>(3, ADD),
                        new Tuple2<>(4, ADD),
                        new Tuple2<>(5, ADD),
                        new Tuple2<>(6, ADD)));
    }

    private void doTest(
            List<Tuple2<Integer, ValueKind>> inputs, List<Tuple2<Integer, ValueKind>> expected)
            throws IOException {
        TestRecordReader reader =
                new TestRecordReader(
                        inputs.stream()
                                .map(
                                        tuple2 ->
                                                new Tuple3<>(
                                                        tuple2.f0,
                                                        tuple2.f1,
                                                        Long.valueOf(tuple2.f0)))
                                .collect(Collectors.toList()),
                        BATCH_SIZE);
        RecordReader<KeyValue> combineReader =
                CombinePolicy.DEDUPLICATE.combine(
                        reader, USER_KEY_COMPARATOR, USER_KEY_SERIALIZER, VALUE_SERIALIZER);

        List<Tuple2<Integer, ValueKind>> values = new ArrayList<>();
        RecordIterator<KeyValue> batch;
        while ((batch = combineReader.readBatch()) != null) {
            while (batch.advanceNext()) {
                values.add(
                        new Tuple2<>(batch.current().key().getInt(0), batch.current().valueKind()));
            }
        }

        combineReader.close();
        Assert.assertTrue(reader.isClosed());

        Assert.assertEquals(expected, values);
    }
}
