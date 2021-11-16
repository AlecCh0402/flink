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

package org.apache.flink.table.storage.filestore.utils;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.filestore.StoreKey;
import org.apache.flink.table.storage.filestore.ValueKind;
import org.apache.flink.table.types.logical.RowType;

/** A {@link ObjectSerializer} for {@link StoreKey}. */
public class StoreKeySerializer extends ObjectSerializer<StoreKey> {

    private final RowDataSerializer userKeySerializer;

    public StoreKeySerializer(RowType userKeyType) {
        super(StoreKey.schema(userKeyType));
        this.userKeySerializer = InternalSerializers.create(userKeyType);
    }

    @Override
    public RowData toRow(StoreKey record) {
        GenericRowData row = new GenericRowData(3);
        row.setField(0, record.userKey());
        row.setField(1, record.sequenceNumber());
        row.setField(2, record.valueKind().toByteValue());
        return row;
    }

    @Override
    public StoreKey fromRow(RowData rowData) {
        return new StoreKey(
                userKeySerializer
                        .toBinaryRow(rowData.getRow(0, userKeySerializer.getArity()))
                        .copy(),
                rowData.getLong(1),
                ValueKind.fromByteValue(rowData.getByte(2)));
    }
}
