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

package org.apache.flink.table.storage.filestore.manifest;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.storage.filestore.KeyValue;
import org.apache.flink.table.storage.filestore.TestKeyValueGenerator;
import org.apache.flink.table.storage.filestore.ValueKind;
import org.apache.flink.table.storage.filestore.lsm.sst.SstTestDataGenerator;
import org.apache.flink.table.storage.filestore.stats.FieldStatsCollector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Random {@link ManifestEntry} generator. */
public class ManifestTestDataGenerator {

    private static final int LEVEL_CAPACITY = 3;

    private final int numBuckets;
    private final List<Map<BinaryRowData, List<List<SstTestDataGenerator.SstFile>>>> levels;
    private final SstTestDataGenerator gen;

    private final LinkedList<ManifestEntry> bufferedResults;

    public ManifestTestDataGenerator() {
        this(3);
    }

    public ManifestTestDataGenerator(int numBuckets) {
        this.numBuckets = numBuckets;
        this.levels = new ArrayList<>();
        for (int i = 0; i < numBuckets; i++) {
            levels.add(new HashMap<>());
        }
        this.gen = new SstTestDataGenerator(numBuckets);

        this.bufferedResults = new LinkedList<>();
    }

    public ManifestEntry next() {
        if (bufferedResults.size() > 0) {
            return bufferedResults.poll();
        }

        SstTestDataGenerator.SstFile file = gen.next();
        List<List<SstTestDataGenerator.SstFile>> bucketLevels =
                levels.get(file.bucket).computeIfAbsent(file.partition, k -> new ArrayList<>());
        ensureCapacity(bucketLevels, file.meta.level());
        List<SstTestDataGenerator.SstFile> level = bucketLevels.get(file.meta.level());
        level.add(file);
        bufferedResults.push(
                new ManifestEntry(
                        ValueKind.ADD, file.partition, file.bucket, numBuckets, file.meta));
        mergeLevelsIfNeeded(file.partition, file.bucket);

        return bufferedResults.poll();
    }

    public ManifestFileMeta createManifestFileMeta(List<ManifestEntry> entries) {
        FieldStatsCollector collector =
                new FieldStatsCollector(TestKeyValueGenerator.PARTITION_TYPE);

        long numAddedFiles = 0;
        long numDeletedFiles = 0;
        for (ManifestEntry entry : entries) {
            collector.collect(entry.partition());
            if (entry.kind() == ValueKind.ADD) {
                numAddedFiles++;
            } else {
                numDeletedFiles++;
            }
        }

        return new ManifestFileMeta(
                "manifest-" + UUID.randomUUID(),
                entries.size() * 100,
                numAddedFiles,
                numDeletedFiles,
                collector.extract());
    }

    private void mergeLevelsIfNeeded(BinaryRowData partition, int bucket) {
        List<List<SstTestDataGenerator.SstFile>> bucketLevels = levels.get(bucket).get(partition);
        int lastModifiedLevel = 0;
        while (bucketLevels.get(lastModifiedLevel).size() > LEVEL_CAPACITY) {

            // remove all sst files in the current and next level
            ensureCapacity(bucketLevels, lastModifiedLevel + 1);
            List<SstTestDataGenerator.SstFile> currentLevel = bucketLevels.get(lastModifiedLevel);
            List<SstTestDataGenerator.SstFile> nextLevel = bucketLevels.get(lastModifiedLevel + 1);
            List<KeyValue> kvs = new ArrayList<>();

            for (SstTestDataGenerator.SstFile file : currentLevel) {
                bufferedResults.push(
                        new ManifestEntry(
                                ValueKind.DELETE, partition, bucket, numBuckets, file.meta));
                kvs.addAll(file.content);
            }
            currentLevel.clear();

            for (SstTestDataGenerator.SstFile file : nextLevel) {
                bufferedResults.push(
                        new ManifestEntry(
                                ValueKind.DELETE, partition, bucket, numBuckets, file.meta));
                kvs.addAll(file.content);
            }
            nextLevel.clear();

            // add back merged sst files
            List<SstTestDataGenerator.SstFile> merged =
                    gen.createSstFiles(kvs, lastModifiedLevel + 1, partition, bucket);
            nextLevel.addAll(merged);
            for (SstTestDataGenerator.SstFile file : nextLevel) {
                bufferedResults.push(
                        new ManifestEntry(ValueKind.ADD, partition, bucket, numBuckets, file.meta));
            }

            lastModifiedLevel += 1;
        }
    }

    private void ensureCapacity(List<List<SstTestDataGenerator.SstFile>> list, int capacity) {
        while (list.size() <= capacity) {
            list.add(new ArrayList<>());
        }
    }
}
