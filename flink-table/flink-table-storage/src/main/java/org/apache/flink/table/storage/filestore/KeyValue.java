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

/**
 * A key value, including user key, sequence number, value kind and value. This object can be
 * reused.
 */
public class KeyValue {

    private RowData key;
    private long sequenceNumber;
    private ValueKind valueKind;
    private RowData value;

    public KeyValue replace(RowData key, long sequenceNumber, ValueKind valueKind, RowData value) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
        this.valueKind = valueKind;
        this.value = value;
        return this;
    }

    public KeyValue setValue(RowData value) {
        this.value = value;
        return this;
    }

    public KeyValue setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
        return this;
    }

    public KeyValue setValueKind(ValueKind valueKind) {
        this.valueKind = valueKind;
        return this;
    }

    public RowData key() {
        return key;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public ValueKind valueKind() {
        return valueKind;
    }

    public RowData value() {
        return value;
    }
}
