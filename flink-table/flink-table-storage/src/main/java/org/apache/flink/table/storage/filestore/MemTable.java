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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.filestore.utils.RecordReader;

import java.io.IOException;

/**
 * Append only memory table for storing key-values. When it is full, it will be flushed to disk and
 * form an SST file.
 */
public interface MemTable {

    /**
     * Put a record with sequence number and value kind.
     *
     * @return True, if the record was successfully written, false, if the mem table was full.
     */
    boolean put(long sequenceNumber, ValueKind valueKind, RowData key, RowData value)
            throws IOException;

    /** Record size of this table. */
    int size();

    /**
     * Returns an iterator over the records in this table. The elements are returned in the order by
     * key and sequence number.
     */
    RecordReader<KeyValue> createOrderedReader();

    /** Removes all records from this table. The table will be empty after this call returns. */
    void clear();
}
