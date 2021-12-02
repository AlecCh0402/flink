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

import org.apache.flink.table.storage.filestore.utils.TestRecordReader;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.storage.filestore.ValueKind.ADD;
import static org.apache.flink.table.storage.filestore.ValueKind.DELETE;
import static org.apache.flink.table.storage.filestore.utils.TestRecordReader.testCombine;
import static org.hamcrest.CoreMatchers.equalTo;

/** Test for {@link ValueCountCombineReader}. */
public class ValueCountCombineReaderTest {

    private static final int BATCH_SIZE = 3;

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testEmpty() throws IOException {
        testCombine(
                CombinePolicy.VALUE_COUNT,
                Collections.emptyList(),
                Collections.emptyList(),
                BATCH_SIZE);
    }

    @Test
    public void testInvalidValueKind() throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Value kind should be ADD."));
        testCombine(
                CombinePolicy.VALUE_COUNT,
                Arrays.asList(
                        new TestRecordReader.TestKeyValue(1, ADD, 1L),
                        new TestRecordReader.TestKeyValue(1, DELETE, 1L)),
                Collections.emptyList(),
                BATCH_SIZE);
    }

    @Test
    public void testInvalidValueCount() throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Value count should not be null."));
        testCombine(
                CombinePolicy.VALUE_COUNT,
                Arrays.asList(
                        new TestRecordReader.TestKeyValue(1, ADD, null),
                        new TestRecordReader.TestKeyValue(1, DELETE, 1L)),
                Collections.emptyList(),
                BATCH_SIZE);
    }

    @Test
    public void testCount() throws IOException {
        testCombine(
                CombinePolicy.VALUE_COUNT,
                Arrays.asList(
                        new TestRecordReader.TestKeyValue(1, ADD, 1L),
                        new TestRecordReader.TestKeyValue(1, ADD, 1L),
                        new TestRecordReader.TestKeyValue(1, ADD, 1L)),
                Collections.singletonList(new TestRecordReader.TestKeyValue(1, ADD, 3L)),
                BATCH_SIZE);
    }

    @Test
    public void testCountCross() throws IOException {
        testCombine(
                CombinePolicy.VALUE_COUNT,
                Arrays.asList(
                        new TestRecordReader.TestKeyValue(1, ADD, 1L),
                        new TestRecordReader.TestKeyValue(1, ADD, 1L),
                        new TestRecordReader.TestKeyValue(1, ADD, 1L),
                        new TestRecordReader.TestKeyValue(1, ADD, 1L),
                        new TestRecordReader.TestKeyValue(1, ADD, 1L),
                        new TestRecordReader.TestKeyValue(1, ADD, 1L),
                        new TestRecordReader.TestKeyValue(1, ADD, 1L)),
                Collections.singletonList(new TestRecordReader.TestKeyValue(1, ADD, 7L)),
                BATCH_SIZE);
    }

    @Test
    public void testMultipleKeys() throws IOException {
        testCombine(
                CombinePolicy.VALUE_COUNT,
                Arrays.asList(
                        new TestRecordReader.TestKeyValue(1, ADD, 1L),
                        new TestRecordReader.TestKeyValue(1, ADD, 2L),
                        new TestRecordReader.TestKeyValue(2, ADD, 1L),
                        new TestRecordReader.TestKeyValue(2, ADD, 1L),
                        new TestRecordReader.TestKeyValue(2, ADD, 1L),
                        new TestRecordReader.TestKeyValue(3, ADD, 1L),
                        new TestRecordReader.TestKeyValue(3, ADD, 1L)),
                Arrays.asList(
                        new TestRecordReader.TestKeyValue(1, ADD, 3L),
                        new TestRecordReader.TestKeyValue(2, ADD, 3L),
                        new TestRecordReader.TestKeyValue(3, ADD, 2L)),
                BATCH_SIZE);
    }
}
