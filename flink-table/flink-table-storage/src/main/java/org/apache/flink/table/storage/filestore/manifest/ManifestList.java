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
import org.apache.flink.table.storage.filestore.utils.FileFormat;
import org.apache.flink.table.storage.filestore.utils.FileUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

/**
 * This file includes several {@link ManifestFileMeta}, representing all data of the whole table at
 * the corresponding snapshot.
 */
public class ManifestList {

    private final Configuration conf;
    private final ManifestFileMetaSerializer serializer;
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final BulkWriter.Factory<RowData> writerFactory;

    public ManifestList(Configuration conf, RowType partitionType, FileFormat fileFormat) {
        this.conf = conf;
        this.serializer = new ManifestFileMetaSerializer(partitionType);
        RowType metaType = ManifestFileMeta.schema(partitionType);
        this.readerFactory = fileFormat.createReaderFactory(metaType);
        this.writerFactory = fileFormat.createWriterFactory(metaType);
    }

    public List<ManifestFileMeta> read(Path path) throws IOException {
        return FileUtils.readListFromFile(path, conf, serializer, readerFactory);
    }

    public void write(List<ManifestFileMeta> metas, Path path) throws IOException {
        Preconditions.checkArgument(
                metas.size() > 0, "Manifest file metas to write must not be empty.");

        FSDataOutputStream out =
                path.getFileSystem().create(path, FileSystem.WriteMode.NO_OVERWRITE);
        BulkWriter<RowData> writer = writerFactory.create(out);
        for (ManifestFileMeta meta : metas) {
            writer.addElement(serializer.toRow(meta));
        }
        writer.finish();
    }
}
