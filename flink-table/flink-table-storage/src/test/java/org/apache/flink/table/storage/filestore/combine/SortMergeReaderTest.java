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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.apache.flink.table.storage.filestore.utils.TestRecordReader.testSortMerge;
import static org.apache.flink.table.storage.filestore.utils.TestRecordReader.testSortMergeWithCombiner;

/** Test for {@link SortMergeReader}. */
@RunWith(Parameterized.class)
public class SortMergeReaderTest {

    @Parameterized.Parameter public CombinePolicy combinePolicy;

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[] {CombinePolicy.DEDUPLICATE, CombinePolicy.VALUE_COUNT};
    }

    @Test
    public void testEmpty() throws IOException {
        List<List<TestRecordReader.TestKeyValue>> inputs = Collections.emptyList();
        testSortMerge(inputs, 1);
        testSortMergeWithCombiner(inputs, combinePolicy, 1);
    }

    @Test
    public void testAlternateKeys() throws IOException {
        List<List<TestRecordReader.TestKeyValue>> inputs =
                Arrays.asList(
                        Arrays.asList(
                                new TestRecordReader.TestKeyValue(1, 1L),
                                new TestRecordReader.TestKeyValue(3, 2L),
                                new TestRecordReader.TestKeyValue(5, 3L),
                                new TestRecordReader.TestKeyValue(7, 4L),
                                new TestRecordReader.TestKeyValue(9, 20L)),
                        Collections.singletonList(new TestRecordReader.TestKeyValue(0, 5L)),
                        Collections.emptyList(),
                        Arrays.asList(
                                new TestRecordReader.TestKeyValue(2, 6L),
                                new TestRecordReader.TestKeyValue(4, 7L),
                                new TestRecordReader.TestKeyValue(6, 8L),
                                new TestRecordReader.TestKeyValue(8, 9L)));
        testSortMerge(inputs, 2);
        testSortMergeWithCombiner(inputs, combinePolicy, 2);
    }

    @Test
    public void testDuplicateKeys() throws IOException {
        List<List<TestRecordReader.TestKeyValue>> inputs1 =
                Arrays.asList(
                        Arrays.asList(
                                new TestRecordReader.TestKeyValue(1, 1L),
                                new TestRecordReader.TestKeyValue(1, 2L),
                                new TestRecordReader.TestKeyValue(3, 3L)),
                        Arrays.asList(
                                new TestRecordReader.TestKeyValue(1, 4L),
                                new TestRecordReader.TestKeyValue(3, 5L)));
        testSortMerge(inputs1, 3);
        testSortMergeWithCombiner(inputs1, combinePolicy, 3);

        List<List<TestRecordReader.TestKeyValue>> inputs2 =
                Arrays.asList(
                        Arrays.asList(
                                new TestRecordReader.TestKeyValue(1, 1L),
                                new TestRecordReader.TestKeyValue(2, 2L),
                                new TestRecordReader.TestKeyValue(3, 3L)),
                        Arrays.asList(
                                new TestRecordReader.TestKeyValue(2, 4L),
                                new TestRecordReader.TestKeyValue(3, 5L),
                                new TestRecordReader.TestKeyValue(4, 6L)),
                        Arrays.asList(
                                new TestRecordReader.TestKeyValue(1, 7L),
                                new TestRecordReader.TestKeyValue(5, 8L)));
        testSortMerge(inputs2, 4);
        testSortMergeWithCombiner(inputs2, combinePolicy, 4);
    }

    @Test
    public void testLongTailRecords() throws IOException {
        List<List<TestRecordReader.TestKeyValue>> inputs =
                Arrays.asList(
                        Arrays.asList(
                                new TestRecordReader.TestKeyValue(1, 1L),
                                new TestRecordReader.TestKeyValue(500, 2L)),
                        Arrays.asList(
                                new TestRecordReader.TestKeyValue(1, 3L),
                                new TestRecordReader.TestKeyValue(3, 4L),
                                new TestRecordReader.TestKeyValue(501, 5L),
                                new TestRecordReader.TestKeyValue(502, 6L),
                                new TestRecordReader.TestKeyValue(503, 7L),
                                new TestRecordReader.TestKeyValue(504, 8L),
                                new TestRecordReader.TestKeyValue(505, 9L),
                                new TestRecordReader.TestKeyValue(506, 10L),
                                new TestRecordReader.TestKeyValue(507, 11L),
                                new TestRecordReader.TestKeyValue(508, 12L),
                                new TestRecordReader.TestKeyValue(509, 13L)));
        testSortMerge(inputs, 5);
        testSortMergeWithCombiner(inputs, combinePolicy, 5);
    }

    @Test
    public void testRandom() throws IOException {
        int numOfReaders = new Random().nextInt(20);
        List<List<TestRecordReader.TestKeyValue>> inputs = new ArrayList<>(numOfReaders);
        for (int i = 0; i < numOfReaders; i++) {
            int numOfRecords = new Random().nextInt(100);
            List<Integer> keys =
                    new Random()
                            .ints(numOfRecords, 0, new Random().nextInt(1000) + 1)
                            .sorted()
                            .boxed()
                            .collect(Collectors.toList());

            long min =
                    i > 0
                            ? inputs.get(i - 1)
                                            .get(inputs.get(i - 1).size() - 1)
                                            .getSequenceNumber()
                                    + 1
                            : 0;
            long max = min + keys.size();
            List<Long> sequenceNumbers =
                    LongStream.range(min, max).boxed().collect(Collectors.toList());
            inputs.add(
                    IntStream.range(0, keys.size())
                            .mapToObj(
                                    j ->
                                            new TestRecordReader.TestKeyValue(
                                                    keys.get(j), sequenceNumbers.get(j)))
                            .collect(Collectors.toList()));
        }
        int batchSize = new Random().nextInt(50) + 1;
        testSortMerge(inputs, batchSize);
        testSortMergeWithCombiner(inputs, combinePolicy, batchSize);
    }
}
