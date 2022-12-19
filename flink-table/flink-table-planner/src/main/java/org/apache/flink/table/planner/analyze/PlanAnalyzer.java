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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;

import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Optional;

/**
 * Plan advisor analyzes the optimized physical plan and gives feedback on potential risk warnings
 * and optimization suggestions.
 */
@Experimental
public interface PlanAnalyzer {
    /** Analyze the optimized {@link RelNode} and return {@link AnalyzedResult}. */
    Optional<AnalyzedResult> analyze(FlinkRelNode rel);

    /** The analyzed {@link PlanAdvice} with a list of applicable {@link RelNode} ids. */
    interface AnalyzedResult {

        PlanAdvice getAdvice();

        List<Integer> getTargetIds();
    }
}
