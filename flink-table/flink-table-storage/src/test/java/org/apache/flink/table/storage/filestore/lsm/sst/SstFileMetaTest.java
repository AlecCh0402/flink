/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.filestore.stats.FieldStats;
import org.apache.flink.table.storage.filestore.utils.ObjectSerializerTestUtils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.io.IOException;

/** Tests for {@link SstFileMeta} and {@link SstFileMetaSerializer}. */
public class SstFileMetaTest {

    private static final RowType ROW_TYPE =
            RowType.of(new IntType(), new VarCharType(10), new ArrayType(new IntType()));
    private static final RowType KEY_TYPE = RowType.of(new IntType(), new VarCharType(10));
    private static final RowDataSerializer KEY_SERIALIZER = new RowDataSerializer(KEY_TYPE);

    @Test
    public void testToFromRow() {
        TestData data = new TestData();
        ObjectSerializerTestUtils.testToFromRow(data.serializer, data.meta, data.rowData);
    }

    @Test
    public void testSerialize() throws IOException {
        TestData data = new TestData();
        ObjectSerializerTestUtils.testSerialize(data.serializer, data.meta);
    }

    private static BinaryRowData key(Integer a, StringData b) {
        return KEY_SERIALIZER.toBinaryRow(GenericRowData.of(a, b)).copy();
    }

    private static class TestData {
        private final SstFileMetaSerializer serializer;
        private final SstFileMeta meta;
        private final RowData rowData;

        private TestData() {
            FieldStats[] stats =
                    new FieldStats[] {
                        new FieldStats(1, 3, 1),
                        new FieldStats(
                                StringData.fromString("Hello"), StringData.fromString("Hi"), 2),
                        new FieldStats(null, null, 0)
                    };
            BinaryRowData minKey = key(null, StringData.fromString("Hi"));
            BinaryRowData maxKey = key(3, StringData.fromString("Hello"));

            serializer = new SstFileMetaSerializer(ROW_TYPE, KEY_TYPE);
            meta =
                    new SstFileMeta(
                            "test", 10000, 100, minKey, maxKey, stats, 20211118, 20221118, 1);
            rowData =
                    GenericRowData.of(
                            StringData.fromString("test"),
                            10000L,
                            100L,
                            minKey,
                            maxKey,
                            GenericRowData.of(
                                    GenericRowData.of(1, StringData.fromString("Hello"), null),
                                    GenericRowData.of(3, StringData.fromString("Hi"), null),
                                    new GenericArrayData(new long[] {1, 2, 0})),
                            20211118L,
                            20221118L,
                            1);
        }
    }
}
