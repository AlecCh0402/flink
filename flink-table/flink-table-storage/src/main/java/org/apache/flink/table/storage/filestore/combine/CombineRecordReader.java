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
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.filestore.KeyValue;
import org.apache.flink.table.storage.filestore.utils.RecordIterator;
import org.apache.flink.table.storage.filestore.utils.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Abstract combine {@link RecordReader}. Combine key-values to key-value. The input {@link
 * RecordReader} is sorted by key and sequence number.
 *
 * <p>This reader needs to hold at least two record objects at the same time to perform a combined
 * process. So the reader only accepts wrapped reader with {@link RecordIterator#supportPrevious()}.
 */
public abstract class CombineRecordReader implements RecordReader<KeyValue> {

    private final RecordReader<KeyValue> reader;
    private final Comparator<RowData> userKeyComparator;
    private final RowDataSerializer userKeySerializer;
    private final RowDataSerializer valueSerializer;

    private KeyValue candidate;

    public CombineRecordReader(
            RecordReader<KeyValue> reader,
            Comparator<RowData> userKeyComparator,
            RowDataSerializer userKeySerializer,
            RowDataSerializer valueSerializer) {
        this.reader = reader;
        this.userKeyComparator = userKeyComparator;
        this.userKeySerializer = userKeySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Nullable
    @Override
    public RecordIterator<KeyValue> readBatch() throws IOException {
        RecordIterator<KeyValue> iterator = reader.readBatch();
        if (iterator != null) {
            return new CombineRecordIterator(iterator);
        } else {
            if (candidate != null) {
                iterator = new SingletonRecordIterator<>(candidate);
                candidate = null;
                return iterator;
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    /** Combine one record to another record with same key. */
    public abstract KeyValue combine(KeyValue from, KeyValue to);

    private class CombineRecordIterator implements RecordIterator<KeyValue> {

        private final RecordIterator<KeyValue> iterator;

        private KeyValue current;

        private CombineRecordIterator(RecordIterator<KeyValue> iterator) {
            checkArgument(iterator.supportPrevious());
            this.iterator = iterator;
        }

        @Override
        public boolean advanceNext() throws IOException {
            if (candidate == null) {
                boolean success = iterator.advanceNext();
                if (!success) {
                    return false;
                }
                candidate = iterator.current();
            }

            while (true) {
                if (iterator.advanceNext()) {
                    // key changed, we can output candidate
                    if (userKeyComparator.compare(iterator.current().key(), candidate.key()) != 0) {
                        current = candidate;
                        candidate = iterator.current();
                        return true;
                    } else {
                        candidate = combine(candidate, iterator.current());
                    }
                } else {
                    // the batch is reused, the batch will be overwritten by readBatch
                    candidate = candidate.copy(userKeySerializer, valueSerializer);
                    // can not return candidate, next batch has records with same key
                    return false;
                }
            }
        }

        @Override
        public boolean supportPrevious() {
            return false;
        }

        @Override
        public KeyValue previous() {
            throw new UnsupportedOperationException();
        }

        @Override
        public KeyValue current() {
            return current;
        }

        @Override
        public void releaseBatch() {
            iterator.releaseBatch();
        }
    }

    private static class SingletonRecordIterator<T> implements RecordIterator<T> {

        private final T value;

        private boolean advanced;

        public SingletonRecordIterator(T value) {
            this.value = value;
            this.advanced = false;
        }

        @Override
        public boolean advanceNext() {
            if (advanced) {
                return false;
            } else {
                advanced = true;
                return true;
            }
        }

        @Override
        public boolean supportPrevious() {
            return false;
        }

        @Override
        public T previous() {
            throw new UnsupportedOperationException();
        }

        @Override
        public T current() {
            return value;
        }

        @Override
        public void releaseBatch() {}
    }
}
