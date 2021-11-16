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

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.filestore.utils.StoreKeySerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Comparator;

/** Test for {@link StoreKey}. */
public class StoreKeyTest {

    private final RowDataSerializer serializer = new RowDataSerializer(new IntType());

    @Test
    public void testCompare() {
        StoreKey key1 = new StoreKey(row(5), 2, ValueKind.ADD);
        StoreKey key2 = new StoreKey(row(5), 5, ValueKind.DELETE);
        StoreKey key3 = new StoreKey(row(3), 2, ValueKind.ADD);
        StoreKey key4 = new StoreKey(row(1), 20, ValueKind.DELETE);
        StoreKey key5 = new StoreKey(row(1), 20, ValueKind.ADD);

        Comparator<RowData> comparator = Comparator.comparingInt(o -> o.getInt(0));
        Assert.assertTrue(StoreKey.compare(comparator, key1, key2) < 0);
        Assert.assertTrue(StoreKey.compare(comparator, key1, key3) > 0);
        Assert.assertTrue(StoreKey.compare(comparator, key2, key3) > 0);
        Assert.assertTrue(StoreKey.compare(comparator, key3, key4) > 0);
        Assert.assertEquals(0, StoreKey.compare(comparator, key5, key4));
    }

    @Test
    public void testToFromRow() {
        StoreKeySerializer storeKeySerializer = new StoreKeySerializer(RowType.of(new IntType()));
        StoreKey key1 = new StoreKey(row(1), 1, ValueKind.ADD);
        StoreKey key2 = new StoreKey(row(2), 2, ValueKind.DELETE);

        RowData row1 = storeKeySerializer.toRow(key1);
        RowData row2 = storeKeySerializer.toRow(key2);

        StoreKey fromKey1 = storeKeySerializer.fromRow(row1);
        StoreKey fromKey2 = storeKeySerializer.fromRow(row2);

        Assert.assertEquals(key1, fromKey1);
        Assert.assertEquals(key2, fromKey2);
    }

    @Test
    public void testSerialize() throws IOException {
        StoreKeySerializer storeKeySerializer = new StoreKeySerializer(RowType.of(new IntType()));
        StoreKey key1 = new StoreKey(row(1), 1, ValueKind.ADD);
        StoreKey key2 = new StoreKey(row(2), 2, ValueKind.DELETE);

        DataOutputSerializer out1 = new DataOutputSerializer(128);
        storeKeySerializer.serialize(key1, out1);

        DataOutputSerializer out2 = new DataOutputSerializer(128);
        storeKeySerializer.serialize(key2, out2);

        StoreKey fromKey1 =
                storeKeySerializer.deserialize(new DataInputDeserializer(out1.getCopyOfBuffer()));
        StoreKey fromKey2 =
                storeKeySerializer.deserialize(new DataInputDeserializer(out2.getCopyOfBuffer()));

        Assert.assertEquals(key1, fromKey1);
        Assert.assertEquals(key2, fromKey2);
    }

    private BinaryRowData row(int i) {
        return serializer.toBinaryRow(GenericRowData.of(i)).copy();
    }
}
