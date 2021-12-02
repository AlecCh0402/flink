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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.filestore.KeyValue;
import org.apache.flink.table.storage.filestore.utils.RecordIterator;
import org.apache.flink.table.storage.filestore.utils.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * This reader is to read a list of {@link RecordReader}, which is already sorted by key and
 * sequence number, and perform a sort merge algorithm. Note that values are not combined at this
 * phase.
 */
public class SortMergeReader implements RecordReader<KeyValue> {

    private final List<RecordReader<KeyValue>> sortedReaders;
    private final Comparator<RowData> userKeyComparator;

    private PriorityQueue<Element> minHeap;
    private Element minElement;

    public SortMergeReader(
            List<RecordReader<KeyValue>> sortedReaders, Comparator<RowData> userKeyComparator) {
        this.sortedReaders = sortedReaders;
        this.userKeyComparator = userKeyComparator;
    }

    @Nullable
    @Override
    public RecordIterator<KeyValue> readBatch() throws IOException {
        if (sortedReaders.isEmpty()) {
            return null;
        }
        if (minHeap == null) {
            initElements();
        } else {
            supplyElements();
        }
        if (minHeap.size() > 0) {
            return new SortMergeIterator();
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        for (RecordReader<KeyValue> reader : sortedReaders) {
            reader.close();
        }
    }

    private void initElements() throws IOException {
        minHeap =
                new PriorityQueue<>(
                        (e1, e2) -> {
                            int result =
                                    userKeyComparator.compare(
                                            e1.iterator.current().key(),
                                            e2.iterator.current().key());
                            if (result != 0) {
                                return result;
                            }
                            return Long.compare(
                                    e1.iterator.current().sequenceNumber(),
                                    e2.iterator.current().sequenceNumber());
                        });
        for (int i = 0; i < sortedReaders.size(); i++) {
            RecordIterator<KeyValue> iterator = sortedReaders.get(i).readBatch();
            if (iterator != null && iterator.advanceNext()) {
                minHeap.offer(new Element(iterator, i));
            }
        }
    }

    private void supplyElements() throws IOException {
        RecordReader<KeyValue> reader = sortedReaders.get(minElement.index);
        RecordIterator<KeyValue> iterator = reader.readBatch();
        if (iterator != null && iterator.advanceNext()) {
            minElement.iterator = iterator;
            minHeap.offer(minElement);
        }
    }

    /** The iterator iterates on {@link SortMergeReader}. */
    private class SortMergeIterator implements RecordIterator<KeyValue> {

        private boolean justPeek = true;

        @Override
        public boolean advanceNext() throws IOException {
            if (minHeap.size() > 0) {
                if (justPeek) {
                    // keep pace with the inner iterators
                    justPeek = false;
                    minElement = minHeap.peek();
                    return true;
                }
                minElement = minHeap.poll();
                // find the top iterator to advance
                RecordIterator<KeyValue> topIterator = minElement.iterator;
                if (topIterator.advanceNext()) {
                    minHeap.offer(minElement);
                    minElement = minHeap.peek();
                    return true;
                } else {
                    // reach the end of current batch, should invoke the next batch
                    return false;
                }
            }
            return false;
        }

        @Override
        public boolean singleInstance() {
            return false;
        }

        @Override
        public KeyValue current() {
            return minElement.iterator.current();
        }

        @Override
        public void releaseBatch() {}
    }

    /**
     * A POJO class composed of a {@link RecordIterator} with the index in the input sorted readers.
     * The index helps track which reader should invoke readBatch operation.
     */
    private static class Element {

        private final int index;

        private RecordIterator<KeyValue> iterator;

        private Element(RecordIterator<KeyValue> iterator, int index) {
            this.iterator = iterator;
            this.index = index;
        }
    }
}
