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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.filestore.KeyValue;
import org.apache.flink.table.storage.filestore.ValueKind;
import org.apache.flink.table.storage.filestore.utils.RecordReader;

import java.util.Comparator;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A value count {@link CombineRecordReader} which sums the count in the value. It only accepts
 * record with {@link ValueKind#ADD} and count cannot be null.
 */
public class ValueCountCombineReader extends CombineRecordReader {

    public ValueCountCombineReader(
            RecordReader<KeyValue> reader,
            Comparator<RowData> userKeyComparator,
            RowDataSerializer userKeySerializer,
            RowDataSerializer valueSerializer) {
        super(reader, userKeyComparator, userKeySerializer, valueSerializer);
    }

    @Override
    public KeyValue combine(KeyValue from, KeyValue to) {
        check(from);
        check(to);
        return to.setValue(GenericRowData.of(count(to) + count(from)));
    }

    private void check(KeyValue keyValue) {
        checkArgument(keyValue.valueKind() == ValueKind.ADD, "Value kind should be ADD.");
        checkArgument(!keyValue.value().isNullAt(0), "Value count should not be null.");
    }

    private long count(KeyValue keyValue) {
        return keyValue.value().getLong(0);
    }
}
