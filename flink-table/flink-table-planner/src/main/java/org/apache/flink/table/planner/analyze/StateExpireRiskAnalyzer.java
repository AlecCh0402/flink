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
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDataStreamScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExpand;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacySink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMatch;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalPythonCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalValues;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** An implementation of {@link PlanAnalyzer} to collect state TTL sensitive rels. */
@Experimental
public class StateExpireRiskAnalyzer implements PlanAnalyzer {

    public static final StateExpireRiskAnalyzer INSTANCE = new StateExpireRiskAnalyzer();

    private static final PlanAdvice STATE_EXPIRE =
            new PlanAdvice(
                    PlanAdvice.Kind.WARNING,
                    PlanAdvice.Scope.LOCAL,
                    String.format(
                            "You might want to pay attention to this node because state TTL configuration '%s' is set.",
                            ExecutionConfigOptions.IDLE_STATE_RETENTION.key()));

    private static final Set<String> STATE_TTL_NOT_SENSITIVE_REL = new HashSet<>();

    static {
        STATE_TTL_NOT_SENSITIVE_REL.add(StreamPhysicalCalc.class.getCanonicalName());
        STATE_TTL_NOT_SENSITIVE_REL.add(StreamPhysicalDataStreamScan.class.getCanonicalName());
        STATE_TTL_NOT_SENSITIVE_REL.add(StreamPhysicalExchange.class.getCanonicalName());
        STATE_TTL_NOT_SENSITIVE_REL.add(StreamPhysicalExpand.class.getCanonicalName());
        STATE_TTL_NOT_SENSITIVE_REL.add(StreamPhysicalLegacySink.class.getCanonicalName());
        STATE_TTL_NOT_SENSITIVE_REL.add(
                StreamPhysicalLegacyTableSourceScan.class.getCanonicalName());
        STATE_TTL_NOT_SENSITIVE_REL.add(StreamPhysicalMatch.class.getCanonicalName());
        STATE_TTL_NOT_SENSITIVE_REL.add(StreamPhysicalPythonCalc.class.getCanonicalName());
        STATE_TTL_NOT_SENSITIVE_REL.add(StreamPhysicalSink.class.getCanonicalName());
        STATE_TTL_NOT_SENSITIVE_REL.add(StreamPhysicalTableSourceScan.class.getCanonicalName());
        STATE_TTL_NOT_SENSITIVE_REL.add(StreamPhysicalValues.class.getCanonicalName());
        STATE_TTL_NOT_SENSITIVE_REL.add(StreamPhysicalUnion.class.getCanonicalName());
    }

    private StateExpireRiskAnalyzer() {}

    @Override
    public Optional<AnalyzedResult> analyze(FlinkRelNode rel) {
        List<Integer> targetRelIds = new ArrayList<>();
        boolean enableStateTTL =
                ShortcutUtils.unwrapTableConfig(rel)
                                .get(ExecutionConfigOptions.IDLE_STATE_RETENTION)
                                .toMillis()
                        > 0;
        if (rel instanceof FlinkPhysicalRel && enableStateTTL) {
            rel.childrenAccept(
                    new RelVisitor() {
                        @Override
                        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                            if (!STATE_TTL_NOT_SENSITIVE_REL.contains(
                                    node.getClass().getCanonicalName())) {
                                targetRelIds.add(node.getId());
                            }
                            super.visit(node, ordinal, parent);
                        }
                    });
            if (!targetRelIds.isEmpty()) {
                return Optional.of(
                        new AnalyzedResult() {
                            @Override
                            public PlanAdvice getAdvice() {
                                return STATE_EXPIRE;
                            }

                            @Override
                            public List<Integer> getTargetIds() {
                                return targetRelIds;
                            }
                        });
            }
        }
        return Optional.empty();
    }
}
