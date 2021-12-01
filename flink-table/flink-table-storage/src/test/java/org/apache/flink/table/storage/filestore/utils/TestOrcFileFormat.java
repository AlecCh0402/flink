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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.orc.OrcColumnarRowFileInputFormat;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.filesystem.FileSystemConnectorOptions;
import org.apache.flink.table.filesystem.PartitionFieldExtractor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.orc.TypeDescription;

import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

/** Orc {@link FileFormat} for tests. */
public class TestOrcFileFormat implements FileFormat {

    @Override
    public BulkFormat<RowData, FileSourceSplit> createReaderFactory(RowType rowType) {
        return OrcColumnarRowFileInputFormat.createPartitionedFormat(
                OrcShim.defaultShim(),
                new org.apache.hadoop.conf.Configuration(),
                rowType,
                Collections.emptyList(),
                PartitionFieldExtractor.forFileSystem(
                        FileSystemConnectorOptions.PARTITION_DEFAULT_NAME.defaultValue()),
                IntStream.range(0, rowType.getFieldCount()).toArray(),
                Collections.emptyList(),
                VectorizedColumnBatch.DEFAULT_SIZE);
    }

    @Override
    public BulkWriter.Factory<RowData> createWriterFactory(RowType rowType) {
        LogicalType[] orcTypes = rowType.getChildren().toArray(new LogicalType[0]);
        TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(rowType);
        return new OrcBulkWriterFactory<>(
                new RowDataVectorizer(typeDescription.toString(), orcTypes),
                new Properties(),
                new org.apache.hadoop.conf.Configuration());
    }
}
