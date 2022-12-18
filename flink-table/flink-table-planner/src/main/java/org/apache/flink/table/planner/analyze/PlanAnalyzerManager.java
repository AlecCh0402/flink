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

package org.apache.flink.table.planner.analyze;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.analyze.factories.PlanAnalyzerFactory;
import org.apache.flink.table.planner.analyze.factories.StreamPlanAnalyzerFactory;

import java.util.ArrayList;
import java.util.List;

/** A manager responsible for create a list of {@link PlanAnalyzer}s. */
@Internal
public class PlanAnalyzerManager {

    private final List<PlanAnalyzer> analyzers;

    public PlanAnalyzerManager() {
        this.analyzers = new ArrayList<>();
        registerAnalyzers();
    }

    public List<PlanAnalyzer> getAnalyzers() {
        return analyzers;
    }

    private void registerAnalyzers() {
        analyzers.addAll(
                FactoryUtil.discoverFactory(
                                Thread.currentThread().getContextClassLoader(),
                                PlanAnalyzerFactory.class,
                                StreamPlanAnalyzerFactory.STREAM_IDENTIFIER)
                        .createAnalyzers());
    }
}
