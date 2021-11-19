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

import java.io.Serializable;
import java.util.Comparator;

/** Combine policy to combine multiple records with the same key. */
public enum CombinePolicy implements Serializable {

    /**
     * Key is the full record. Value is a count which represents number of records of the exact same
     * fields.
     */
    VALUE_COUNT,

    /** Key is primary key (unique), value is the full record, only keep the latest one. */
    DEDUPLICATE;

    /**
     * Combine key-values to key-value. The input {@link RecordReader} is sorted by key and sequence
     * number.
     */
    public RecordReader<KeyValue> combine(
            RecordReader<KeyValue> iterator,
            Comparator<RowData> comparator,
            RowDataSerializer userKeySerializer,
            RowDataSerializer valueSerializer) {
        switch (this) {
            case VALUE_COUNT:
                return new ValueCountCombineReader(
                        iterator, comparator, userKeySerializer, valueSerializer);
            case DEDUPLICATE:
                return new DeduplicateCombineReader(
                        iterator, comparator, userKeySerializer, valueSerializer);
            default:
                throw new UnsupportedOperationException("Unsupported strategy: " + this);
        }
    }
}
