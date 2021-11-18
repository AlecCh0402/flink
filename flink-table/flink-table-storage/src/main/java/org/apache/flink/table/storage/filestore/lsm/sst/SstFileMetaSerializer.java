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

package org.apache.flink.table.storage.filestore.lsm.sst;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.filestore.stats.FieldStats;
import org.apache.flink.table.storage.filestore.utils.ObjectSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.util.stream.IntStream;

/** Serializer for {@link SstFileMeta}. */
public class SstFileMetaSerializer extends ObjectSerializer<SstFileMeta> {

    private final int rowFieldCount;
    private final int keyFieldCount;
    private final RowData.FieldGetter[] fieldGetters;
    private final RowDataSerializer keySerializer;

    public SstFileMetaSerializer(RowType rowType, RowType keyType) {
        super(SstFileMeta.schema(rowType, keyType));
        this.rowFieldCount = rowType.getFieldCount();
        this.keyFieldCount = keyType.getFieldCount();
        this.fieldGetters =
                IntStream.range(0, rowFieldCount)
                        .mapToObj(i -> RowData.createFieldGetter(rowType.getTypeAt(i), i))
                        .toArray(RowData.FieldGetter[]::new);
        this.keySerializer = new RowDataSerializer(keyType);
    }

    @Override
    public RowData toRow(SstFileMeta meta) {
        return GenericRowData.of(
                StringData.fromString(meta.name()),
                meta.fileSize(),
                meta.rowCount(),
                meta.minKey(),
                meta.maxKey(),
                statsToRow(meta),
                meta.minSequenceNumber(),
                meta.maxSequenceNumber(),
                meta.level());
    }

    private RowData statsToRow(SstFileMeta meta) {
        GenericRowData minValues = new GenericRowData(rowFieldCount);
        GenericRowData maxValues = new GenericRowData(rowFieldCount);
        long[] nullCounts = new long[rowFieldCount];
        for (int i = 0; i < rowFieldCount; i++) {
            FieldStats stats = meta.stats()[i];
            minValues.setField(i, stats.minValue());
            maxValues.setField(i, stats.maxValue());
            nullCounts[i] = stats.nullCount();
        }
        return GenericRowData.of(minValues, maxValues, new GenericArrayData(nullCounts));
    }

    @Override
    public SstFileMeta fromRow(RowData row) {
        return new SstFileMeta(
                row.getString(0).toString(),
                row.getLong(1),
                row.getLong(2),
                keySerializer.toBinaryRow(row.getRow(3, keyFieldCount)).copy(),
                keySerializer.toBinaryRow(row.getRow(4, keyFieldCount)).copy(),
                statsFromRow(row),
                row.getLong(6),
                row.getLong(7),
                row.getInt(8));
    }

    private FieldStats[] statsFromRow(RowData row) {
        RowData statsRow = row.getRow(5, 3);
        RowData minValues = statsRow.getRow(0, rowFieldCount);
        RowData maxValues = statsRow.getRow(1, rowFieldCount);
        long[] nullValues = statsRow.getArray(2).toLongArray();

        FieldStats[] stats = new FieldStats[rowFieldCount];
        for (int i = 0; i < rowFieldCount; i++) {
            stats[i] =
                    new FieldStats(
                            fieldGetters[i].getFieldOrNull(minValues),
                            fieldGetters[i].getFieldOrNull(maxValues),
                            nullValues[i]);
        }
        return stats;
    }
}
