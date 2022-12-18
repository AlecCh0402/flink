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
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.PartialFinalType;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;
import org.apache.flink.table.planner.plan.trait.RelWindowProperties;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.WindowUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * An implementation of {@link PlanAnalyzer} to analyze whether {@link
 * org.apache.flink.table.planner.plan.rules.logical.SplitAggregateRule} can be applied.
 */
@Experimental
public class SplitDistinctAggAnalyzer implements PlanAnalyzer {

    public static final SplitDistinctAggAnalyzer INSTANCE = new SplitDistinctAggAnalyzer();

    private static final PlanAdvice SPLIT_AGG =
            new PlanAdvice(
                    SplitDistinctAggAnalyzer.class.getSimpleName(),
                    String.format(
                            "You might want to set '%s' = 'true' to enable distinct aggregate optimization.",
                            OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED
                                    .key()),
                    PlanAdvice.AdviceKind.HINT);

    private SplitDistinctAggAnalyzer() {}

    @Override
    public Optional<AnalyzedResult> analyze(FlinkRelNode rel) {
        List<Integer> targetRelIds = new ArrayList<>();
        if (rel instanceof FlinkPhysicalRel) {
            rel.childrenAccept(
                    new RelVisitor() {
                        @Override
                        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                            if (node instanceof StreamPhysicalGroupAggregateBase) {
                                if (matches((StreamPhysicalGroupAggregateBase) node)) {
                                    targetRelIds.add(node.getId());
                                }
                            }
                            super.visit(node, ordinal, parent);
                        }
                    });
            if (!targetRelIds.isEmpty()) {
                return Optional.of(
                        new AnalyzedResult() {
                            @Override
                            public PlanAdvice advice() {
                                return SPLIT_AGG;
                            }

                            @Override
                            public List<Integer> targetIds() {
                                return targetRelIds;
                            }
                        });
            }
        }
        return Optional.empty();
    }

    private boolean matches(StreamPhysicalGroupAggregateBase groupAggregate) {
        boolean splitDistinctAggEnabled =
                ShortcutUtils.unwrapTableConfig(groupAggregate)
                        .get(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED);
        List<AggregateCall> aggCalls =
                scala.collection.JavaConverters.seqAsJavaList(groupAggregate.aggCalls());
        boolean isAggContainDistinct = aggCalls.stream().anyMatch(AggregateCall::isDistinct);
        boolean isAllAggSplittable = AggregateUtil.doAllAggSupportSplit(aggCalls);
        boolean isTableAgg = AggregateUtil.isTableAggregate(aggCalls);

        FlinkRelMetadataQuery fmq =
                (FlinkRelMetadataQuery) groupAggregate.getCluster().getMetadataQuery();
        RelWindowProperties windowProperties =
                fmq.getRelWindowProperties(groupAggregate.getInput());
        boolean isWindowAgg =
                WindowUtil.groupingContainsWindowStartEnd(
                        ImmutableBitSet.of(groupAggregate.grouping()), windowProperties);
        boolean isProctimeWindowAgg = isWindowAgg && !windowProperties.isRowtime();
        boolean isPartialFinalNone;
        if (groupAggregate instanceof StreamPhysicalGroupAggregate) {
            isPartialFinalNone =
                    ((StreamPhysicalGroupAggregate) groupAggregate).partialFinalType()
                            == PartialFinalType.NONE;
        } else if (groupAggregate instanceof StreamPhysicalLocalGroupAggregate) {
            isPartialFinalNone =
                    ((StreamPhysicalLocalGroupAggregate) groupAggregate).partialFinalType()
                            == PartialFinalType.NONE;
        } else if (groupAggregate instanceof StreamPhysicalGlobalGroupAggregate) {
            isPartialFinalNone =
                    ((StreamPhysicalGlobalGroupAggregate) groupAggregate).partialFinalType()
                            == PartialFinalType.NONE;
        } else {
            isPartialFinalNone = false;
        }
        return !splitDistinctAggEnabled
                && isAggContainDistinct
                && isAllAggSplittable
                && !isProctimeWindowAgg
                && !isTableAgg
                && isPartialFinalNone;
    }
}
