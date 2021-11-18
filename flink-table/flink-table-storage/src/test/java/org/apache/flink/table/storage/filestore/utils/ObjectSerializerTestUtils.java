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

package org.apache.flink.table.storage.filestore.utils;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;

import org.junit.Assert;

import java.io.IOException;

/** Test utils for {@link ObjectSerializer} and its subclasses. */
public class ObjectSerializerTestUtils {

    public static <T> void testToFromRow(ObjectSerializer<T> serializer, T object, RowData row) {
        Assert.assertEquals(row, serializer.toRow(object));
        Assert.assertEquals(object, serializer.fromRow(row));
    }

    public static <T> void testSerialize(ObjectSerializer<T> serializer, T object)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(128);
        serializer.serialize(object, out);
        T actual = serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
        Assert.assertEquals(object, actual);
    }
}
