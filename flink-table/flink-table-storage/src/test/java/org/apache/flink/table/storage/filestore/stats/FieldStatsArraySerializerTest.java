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

package org.apache.flink.table.storage.filestore.stats;

import org.apache.flink.table.storage.filestore.TestKeyValueGenerator;
import org.apache.flink.table.storage.filestore.utils.ObjectSerializer;
import org.apache.flink.table.storage.filestore.utils.ObjectSerializerTestBase;

import org.junit.Assert;

/** Tests for {@link FieldStatsArraySerializer}. */
public class FieldStatsArraySerializerTest extends ObjectSerializerTestBase<FieldStats[]> {

    public TestKeyValueGenerator gen;

    public FieldStatsArraySerializerTest() {
        this.gen = new TestKeyValueGenerator();
    }

    @Override
    protected ObjectSerializer<FieldStats[]> serializer() {
        return new FieldStatsArraySerializer(TestKeyValueGenerator.ROW_TYPE);
    }

    @Override
    protected FieldStats[] object() {
        FieldStatsCollector collector = new FieldStatsCollector(TestKeyValueGenerator.ROW_TYPE);
        for (int i = 0; i < 10; i++) {
            collector.collect(gen.next().value());
        }
        return collector.extract();
    }

    @Override
    protected void checkResult(FieldStats[] expected, FieldStats[] actual) {
        Assert.assertArrayEquals(expected, actual);
    }
}
