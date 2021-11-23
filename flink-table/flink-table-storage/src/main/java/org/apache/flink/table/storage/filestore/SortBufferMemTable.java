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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.storage.filestore.KeyValue.FromRowConverter;
import org.apache.flink.table.storage.filestore.KeyValue.ToRowConverter;
import org.apache.flink.table.storage.filestore.utils.HeapMemorySegmentPool;
import org.apache.flink.table.storage.filestore.utils.RecordIterator;
import org.apache.flink.table.storage.filestore.utils.RecordReader;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/** A {@link MemTable} which stores records in {@link BinaryInMemorySortBuffer}. */
public class SortBufferMemTable implements MemTable {

    public static final int PAGE_SIZE = (int) MemorySize.ofMebiBytes(1).getBytes();

    private final int keyArity;

    private final int valueArity;

    private final ToRowConverter rowConverter;

    private final BinaryInMemorySortBuffer buffer;

    public SortBufferMemTable(RowType keyType, RowType valueType, long maxMemSize) {
        this.keyArity = keyType.getFieldCount();
        this.valueArity = valueType.getFieldCount();
        this.rowConverter = new ToRowConverter();

        // user key + sequenceNumber
        List<LogicalType> sortKeyTypes = new ArrayList<>(keyType.getChildren());
        sortKeyTypes.add(new BigIntType(false));

        // for sort binary buffer
        SortCodeGenerator sortCodeGenerator =
                new SortCodeGenerator(
                        new TableConfig(),
                        RowType.of(sortKeyTypes.toArray(new LogicalType[0])),
                        SortUtil.getAscendingSortSpec(
                                IntStream.range(0, sortKeyTypes.size()).toArray()));
        NormalizedKeyComputer normalizedKeyComputer =
                sortCodeGenerator
                        .generateNormalizedKeyComputer("MemTableKeyComputer")
                        .newInstance(Thread.currentThread().getContextClassLoader());
        RecordComparator keyComparator =
                sortCodeGenerator
                        .generateRecordComparator("MemTableComparator")
                        .newInstance(Thread.currentThread().getContextClassLoader());

        this.buffer =
                BinaryInMemorySortBuffer.createBuffer(
                        normalizedKeyComputer,
                        InternalSerializers.create(KeyValue.schema(keyType, valueType)),
                        new BinaryRowDataSerializer(sortKeyTypes.size()),
                        keyComparator,
                        new HeapMemorySegmentPool((int) (maxMemSize / PAGE_SIZE), PAGE_SIZE));
    }

    @Override
    public boolean put(long sequenceNumber, ValueKind valueKind, RowData key, RowData value)
            throws IOException {
        return buffer.write(rowConverter.replace(key, sequenceNumber, valueKind, value));
    }

    @Override
    public int size() {
        return buffer.size();
    }

    @Override
    public RecordReader<KeyValue> createOrderedReader() {
        new QuickSort().sort(buffer);

        MutableObjectIterator<BinaryRowData> kvIter = buffer.getIterator();

        RecordIterator<KeyValue> iterator =
                new RecordIterator<KeyValue>() {

                    FromRowConverter<BinaryRowData> previous = createConverter();
                    FromRowConverter<BinaryRowData> current = createConverter();

                    private FromRowConverter<BinaryRowData> createConverter() {
                        return new FromRowConverter<>(keyArity, valueArity);
                    }

                    @Override
                    public boolean advanceNext() throws IOException {
                        switchKeyValue();

                        BinaryRowData row =
                                kvIter.next(
                                        current.row() == null
                                                ? new BinaryRowData(keyArity + 2 + valueArity)
                                                : current.row());
                        if (row == null) {
                            return false;
                        }
                        current.replace(row);
                        return true;
                    }

                    @Override
                    public boolean supportPrevious() {
                        return true;
                    }

                    @Override
                    public KeyValue previous() {
                        return previous.record();
                    }

                    @Override
                    public KeyValue current() {
                        return current.record();
                    }

                    @Override
                    public void releaseBatch() {}

                    private void switchKeyValue() {
                        FromRowConverter<BinaryRowData> tmp = previous;
                        previous = current;
                        current = tmp;
                    }
                };

        return new RecordReader<KeyValue>() {

            private boolean returned = false;

            @Override
            public RecordIterator<KeyValue> readBatch() {
                if (returned) {
                    return null;
                } else {
                    returned = true;
                    return iterator;
                }
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public void clear() {
        buffer.reset();
    }
}
