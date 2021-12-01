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

package org.apache.flink.table.storage.filestore.manifest;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.filestore.stats.FieldStatsCollector;
import org.apache.flink.table.storage.filestore.utils.FileFormat;
import org.apache.flink.table.storage.filestore.utils.FileUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

/**
 * This file includes several {@link ManifestEntry}s, representing the additional changes since last
 * snapshot.
 */
public class ManifestFile {

    private final Configuration conf;
    private final RowType partitionType;
    private final ManifestEntrySerializer serializer;
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final BulkWriter.Factory<RowData> writerFactory;

    public ManifestFile(
            Configuration conf,
            RowType partitionType,
            RowType rowType,
            RowType keyType,
            FileFormat fileFormat) {
        this.conf = conf;
        this.partitionType = partitionType;
        this.serializer = new ManifestEntrySerializer(partitionType, rowType, keyType);
        RowType entryType = ManifestEntry.schema(partitionType, rowType, keyType);
        this.readerFactory = fileFormat.createReaderFactory(entryType);
        this.writerFactory = fileFormat.createWriterFactory(entryType);
    }

    public List<ManifestEntry> read(Path path) throws IOException {
        return FileUtils.readListFromFile(path, conf, serializer, readerFactory);
    }

    public ManifestFileMeta write(List<ManifestEntry> entries, Path path) throws IOException {
        Preconditions.checkArgument(
                entries.size() > 0, "Manifest entries to write must not be empty.");

        FSDataOutputStream out =
                path.getFileSystem().create(path, FileSystem.WriteMode.NO_OVERWRITE);
        BulkWriter<RowData> writer = writerFactory.create(out);
        long numAddedFiles = 0;
        long numDeletedFiles = 0;
        FieldStatsCollector statsCollector = new FieldStatsCollector(partitionType);

        for (ManifestEntry entry : entries) {
            writer.addElement(serializer.toRow(entry));
            switch (entry.kind()) {
                case ADD:
                    numAddedFiles++;
                    break;
                case DELETE:
                    numDeletedFiles++;
                    break;
            }
            statsCollector.collect(entry.partition());
        }
        writer.finish();

        return new ManifestFileMeta(
                path.getName(),
                path.getFileSystem().getFileStatus(path).getLen(),
                numAddedFiles,
                numDeletedFiles,
                statsCollector.extract());
    }
}
