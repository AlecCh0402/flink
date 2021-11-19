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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.storage.filestore.KeyValue;
import org.apache.flink.table.storage.filestore.ValueKind;
import org.apache.flink.table.storage.filestore.utils.RecordIterator;
import org.apache.flink.table.storage.filestore.utils.RecordReader;
import org.apache.flink.table.storage.filestore.utils.TestRecordReader;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.storage.filestore.utils.TestRecordReader.USER_KEY_COMPARATOR;
import static org.apache.flink.table.storage.filestore.utils.TestRecordReader.USER_KEY_SERIALIZER;
import static org.apache.flink.table.storage.filestore.utils.TestRecordReader.VALUE_SERIALIZER;
import static org.hamcrest.CoreMatchers.equalTo;

/** Test for {@link ValueCountCombineReader}. */
public class ValueCountCombineReaderTest {

    private static final int BATCH_SIZE = 3;

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testEmpty() throws IOException {
        doTest(Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void testInvalidValueKind() throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Value kind should be ADD."));
        doTest(
                Arrays.asList(
                        new Tuple3<>(1, ValueKind.ADD, 1L), new Tuple3<>(1, ValueKind.DELETE, 1L)),
                Collections.emptyList());
    }

    @Test
    public void testInvalidValueCount() throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Value count should not be null."));
        doTest(
                Arrays.asList(
                        new Tuple3<>(1, ValueKind.ADD, null),
                        new Tuple3<>(1, ValueKind.DELETE, 1L)),
                Collections.emptyList());
    }

    @Test
    public void testCount() throws IOException {
        doTest(
                Arrays.asList(
                        new Tuple3<>(1, ValueKind.ADD, 1L),
                        new Tuple3<>(1, ValueKind.ADD, 1L),
                        new Tuple3<>(1, ValueKind.ADD, 1L)),
                Collections.singletonList(new Tuple3<>(1, ValueKind.ADD, 3L)));
    }

    @Test
    public void testCountCross() throws IOException {
        doTest(
                Arrays.asList(
                        new Tuple3<>(1, ValueKind.ADD, 1L),
                        new Tuple3<>(1, ValueKind.ADD, 1L),
                        new Tuple3<>(1, ValueKind.ADD, 1L),
                        new Tuple3<>(1, ValueKind.ADD, 1L),
                        new Tuple3<>(1, ValueKind.ADD, 1L),
                        new Tuple3<>(1, ValueKind.ADD, 1L),
                        new Tuple3<>(1, ValueKind.ADD, 1L)),
                Collections.singletonList(new Tuple3<>(1, ValueKind.ADD, 7L)));
    }

    @Test
    public void testMultipleKeys() throws IOException {
        doTest(
                Arrays.asList(
                        new Tuple3<>(1, ValueKind.ADD, 1L),
                        new Tuple3<>(1, ValueKind.ADD, 2L),
                        new Tuple3<>(2, ValueKind.ADD, 1L),
                        new Tuple3<>(2, ValueKind.ADD, 1L),
                        new Tuple3<>(2, ValueKind.ADD, 1L),
                        new Tuple3<>(3, ValueKind.ADD, 1L),
                        new Tuple3<>(3, ValueKind.ADD, 1L)),
                Arrays.asList(
                        new Tuple3<>(1, ValueKind.ADD, 3L),
                        new Tuple3<>(2, ValueKind.ADD, 3L),
                        new Tuple3<>(3, ValueKind.ADD, 2L)));
    }

    private void doTest(
            List<Tuple3<Integer, ValueKind, Long>> inputs,
            List<Tuple3<Integer, ValueKind, Long>> expected)
            throws IOException {
        TestRecordReader reader = new TestRecordReader(inputs, BATCH_SIZE);
        RecordReader<KeyValue> combineReader =
                CombinePolicy.VALUE_COUNT.combine(
                        reader, USER_KEY_COMPARATOR, USER_KEY_SERIALIZER, VALUE_SERIALIZER);

        List<Tuple3<Integer, ValueKind, Long>> values = new ArrayList<>();
        RecordIterator<KeyValue> batch;
        while ((batch = combineReader.readBatch()) != null) {
            while (batch.advanceNext()) {
                values.add(
                        new Tuple3<>(
                                batch.current().key().getInt(0),
                                batch.current().valueKind(),
                                batch.current().value().getLong(0)));
            }
        }

        combineReader.close();
        Assert.assertTrue(reader.isClosed());

        Assert.assertEquals(expected, values);
    }
}
