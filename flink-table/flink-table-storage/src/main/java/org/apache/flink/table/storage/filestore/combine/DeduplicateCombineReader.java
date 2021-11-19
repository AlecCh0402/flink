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
import org.apache.flink.table.storage.filestore.utils.RecordReader;

import java.util.Comparator;

/** A deduplicate {@link CombineRecordReader} which keeps the latest record of the current key. */
public class DeduplicateCombineReader extends CombineRecordReader {

    public DeduplicateCombineReader(
            RecordReader<KeyValue> reader,
            Comparator<RowData> userKeyComparator,
            RowDataSerializer userKeySerializer,
            RowDataSerializer valueSerializer) {
        super(reader, userKeyComparator, userKeySerializer, valueSerializer);
    }

    @Override
    public KeyValue combine(KeyValue from, KeyValue to) {
        return to;
    }
}
