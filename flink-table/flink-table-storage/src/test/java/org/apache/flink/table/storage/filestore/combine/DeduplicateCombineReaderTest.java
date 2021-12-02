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

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.storage.filestore.ValueKind.ADD;
import static org.apache.flink.table.storage.filestore.ValueKind.DELETE;
import static org.apache.flink.table.storage.filestore.utils.TestRecordReader.testCombine;

/** Test for {@link DeduplicateCombineReader}. */
public class DeduplicateCombineReaderTest {

    private static final int BATCH_SIZE = 3;

    @Test
    public void testEmpty() throws IOException {
        testCombine(
                CombinePolicy.DEDUPLICATE,
                Collections.emptyList(),
                Collections.emptyList(),
                BATCH_SIZE);
    }

    @Test
    public void testDelete() throws IOException {
        testCombine(
                CombinePolicy.DEDUPLICATE,
                Arrays.asList(
                        new TestRecordReader.TestKeyValue(1, ADD),
                        new TestRecordReader.TestKeyValue(1, ADD),
                        new TestRecordReader.TestKeyValue(1, DELETE)),
                Collections.singletonList(new TestRecordReader.TestKeyValue(1, DELETE)),
                BATCH_SIZE);
    }

    @Test
    public void testCrossBatch() throws IOException {
        testCombine(
                CombinePolicy.DEDUPLICATE,
                Arrays.asList(
                        new TestRecordReader.TestKeyValue(1, ADD),
                        new TestRecordReader.TestKeyValue(1, ADD),
                        new TestRecordReader.TestKeyValue(1, ADD),
                        new TestRecordReader.TestKeyValue(1, DELETE)),
                Collections.singletonList(new TestRecordReader.TestKeyValue(1, DELETE)),
                BATCH_SIZE);
    }

    @Test
    public void testMultipleKeys() throws IOException {
        testCombine(
                CombinePolicy.DEDUPLICATE,
                Arrays.asList(
                        new TestRecordReader.TestKeyValue(1, ADD),
                        new TestRecordReader.TestKeyValue(2, ADD),
                        new TestRecordReader.TestKeyValue(2, DELETE),
                        new TestRecordReader.TestKeyValue(3, ADD)),
                Arrays.asList(
                        new TestRecordReader.TestKeyValue(1, ADD),
                        new TestRecordReader.TestKeyValue(2, DELETE),
                        new TestRecordReader.TestKeyValue(3, ADD)),
                BATCH_SIZE);
    }

    @Test
    public void testMultipleKeysCross() throws IOException {
        testCombine(
                CombinePolicy.DEDUPLICATE,
                Arrays.asList(
                        new TestRecordReader.TestKeyValue(1, ADD),
                        new TestRecordReader.TestKeyValue(2, ADD),
                        new TestRecordReader.TestKeyValue(2, ADD),
                        new TestRecordReader.TestKeyValue(2, DELETE),
                        new TestRecordReader.TestKeyValue(3, ADD),
                        new TestRecordReader.TestKeyValue(4, ADD),
                        new TestRecordReader.TestKeyValue(5, ADD),
                        new TestRecordReader.TestKeyValue(6, ADD)),
                Arrays.asList(
                        new TestRecordReader.TestKeyValue(1, ADD),
                        new TestRecordReader.TestKeyValue(2, DELETE),
                        new TestRecordReader.TestKeyValue(3, ADD),
                        new TestRecordReader.TestKeyValue(4, ADD),
                        new TestRecordReader.TestKeyValue(5, ADD),
                        new TestRecordReader.TestKeyValue(6, ADD)),
                BATCH_SIZE);
    }
}
