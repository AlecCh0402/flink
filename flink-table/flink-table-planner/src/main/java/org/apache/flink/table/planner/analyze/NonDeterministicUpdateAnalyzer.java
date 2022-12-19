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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.optimize.StreamNonDeterministicUpdatePlanVisitor;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.UpsertMaterialize.FORCE;

/** An implementation of {@link PlanAnalyzer} to analyze the risk of non-deterministic update. */
@Experimental
public class NonDeterministicUpdateAnalyzer implements PlanAnalyzer {

    public static final NonDeterministicUpdateAnalyzer INSTANCE =
            new NonDeterministicUpdateAnalyzer();

    private static final StreamNonDeterministicUpdatePlanVisitor NDU_VISITOR =
            new StreamNonDeterministicUpdatePlanVisitor();

    private static final PlanAdvice NDU_RISK =
            new PlanAdvice(PlanAdvice.Kind.WARNING, PlanAdvice.Scope.GLOBAL);

    private NonDeterministicUpdateAnalyzer() {}

    @Override
    public Optional<AnalyzedResult> analyze(FlinkRelNode rel) {
        boolean ignoreNDU =
                ShortcutUtils.unwrapTableConfig(rel)
                                .get(
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY)
                        == OptimizerConfigOptions.NonDeterministicUpdateStrategy.IGNORE;
        if (rel instanceof StreamPhysicalRel && ignoreNDU) {
            String content = null;
            try {
                StreamPhysicalRel updateRel = NDU_VISITOR.visit((StreamPhysicalRel) rel);
                if (!updateRel.deepEquals(rel)) { // might be rewritten by visitor
                    content =
                            String.format(
                                    "You might want to either explicit set '%s' = '%s' or turn on '%s'"
                                            + "to avoid data correctness risk caused by the look up join operator.",
                                    TABLE_EXEC_SINK_UPSERT_MATERIALIZE.key(),
                                    FORCE.name(),
                                    OptimizerConfigOptions
                                            .TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY
                                            .key());
                }
            } catch (TableException e) {
                content = e.getMessage();
            }
            if (content != null) {
                String finalContent = content;
                return Optional.of(
                        new AnalyzedResult() {
                            @Override
                            public PlanAdvice getAdvice() {
                                return NDU_RISK.withContent(finalContent);
                            }

                            @Override
                            public List<Integer> getTargetIds() {
                                return Collections.emptyList();
                            }
                        });
            }
        }

        return Optional.empty();
    }
}
