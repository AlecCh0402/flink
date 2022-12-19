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

package org.apache.flink.table.planner.analyze.factories;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.planner.analyze.NonDeterministicUpdateAnalyzer;
import org.apache.flink.table.planner.analyze.PlanAnalyzer;
import org.apache.flink.table.planner.analyze.SplitDistinctAggAnalyzer;
import org.apache.flink.table.planner.analyze.StateExpireRiskAnalyzer;

import java.util.Arrays;
import java.util.List;

/**
 * The factory to create a collection of {@link PlanAnalyzer}s to analyze optimized physical plan
 * under stream mode.
 */
@Experimental
public class StreamPlanAnalyzerFactory implements PlanAnalyzerFactory {

    public static final String STREAM_IDENTIFIER = "stream-plan-analyzer";

    @Override
    public String factoryIdentifier() {
        return STREAM_IDENTIFIER;
    }

    @Override
    public List<PlanAnalyzer> createAnalyzers() {
        return Arrays.asList(
                StateExpireRiskAnalyzer.INSTANCE,
                SplitDistinctAggAnalyzer.INSTANCE,
                NonDeterministicUpdateAnalyzer.INSTANCE);
    }
}
