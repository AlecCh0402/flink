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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.connectors.DynamicSourceUtils;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalcBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeduplicate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExpand;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLegacySink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLimit;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSort;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSortLimit;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalSort;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.OverAggregateUtil;
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** An implementation of {@link PlanAnalyzer} to analyze the risk of non-deterministic update. */
@Experimental
public class NonDeterministicUpdateAnalyzer implements PlanAnalyzer {

    public static final NonDeterministicUpdateAnalyzer INSTANCE =
            new NonDeterministicUpdateAnalyzer();

    private static final PlanAdvice NDU_RISK =
            new PlanAdvice(
                    NonDeterministicUpdateAnalyzer.class.getSimpleName(),
                    PlanAdvice.AdviceKind.WARNING);

    private NonDeterministicUpdateAnalyzer() {}

    @Override
    public Optional<AnalyzedResult> analyze(FlinkRelNode rel) {
        List<Integer> targetRelIds = new ArrayList<>();
        boolean ignoreNDU =
                ShortcutUtils.unwrapTableConfig(rel)
                                .get(
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY)
                        == OptimizerConfigOptions.NonDeterministicUpdateStrategy.IGNORE;
        if (rel instanceof FlinkPhysicalRel && ignoreNDU) {
            matches(rel, ImmutableBitSet.of())
                    .ifPresent(
                            tuple -> {
                                targetRelIds.add(tuple.f0);
                                NDU_RISK.withContent(tuple.f1);
                            });
            if (!targetRelIds.isEmpty()) {
                return Optional.of(
                        new AnalyzedResult() {
                            @Override
                            public PlanAdvice advice() {
                                return NDU_RISK;
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

    // TODO: should reuse StreamNonDeterministicUpdatePlanVisitor
    private Optional<Tuple2<Integer, String>> matches(
            RelNode node, ImmutableBitSet requireDeterminism) {
        if (node instanceof StreamPhysicalSink) {
            return matches((StreamPhysicalSink) node);
        } else if (node instanceof StreamPhysicalLegacySink<?>) {
            return matches((StreamPhysicalLegacySink<?>) node);
        } else if (node instanceof StreamPhysicalCalcBase) {
            return matches((StreamPhysicalCalcBase) node, requireDeterminism);
        } else if (node instanceof StreamPhysicalCorrelateBase) {
            return matches((StreamPhysicalCorrelateBase) node, requireDeterminism);
        } else if (node instanceof StreamPhysicalLookupJoin) {
            return matches((StreamPhysicalLookupJoin) node, requireDeterminism);
        } else if (node instanceof StreamPhysicalTableSourceScan) {
            return matches((StreamPhysicalTableSourceScan) node, requireDeterminism);
        } else if (node instanceof StreamPhysicalGroupAggregateBase) {
            return matches((StreamPhysicalGroupAggregateBase) node, requireDeterminism);
        } else if (node instanceof StreamPhysicalWindowAggregateBase) {
            return matches((StreamPhysicalWindowAggregateBase) node, requireDeterminism);
        } else if (node instanceof StreamPhysicalExpand) {
            return matches((StreamPhysicalExpand) node, requireDeterminism);
        } else if (node instanceof CommonPhysicalJoin) {
            return matches((CommonPhysicalJoin) node, requireDeterminism);
        } else if (node instanceof StreamPhysicalOverAggregateBase) {
            return matches((StreamPhysicalOverAggregateBase) node, requireDeterminism);
        } else if (node instanceof StreamPhysicalRank) {
            return matches((StreamPhysicalRank) node, requireDeterminism);
        } else if (node instanceof StreamPhysicalDeduplicate) {
            return matches((StreamPhysicalDeduplicate) node, requireDeterminism);
        } else if (node instanceof StreamPhysicalWindowDeduplicate) {
            return matches((StreamPhysicalWindowDeduplicate) node, requireDeterminism);
        } else if (node instanceof StreamPhysicalWindowRank) {
            return matches((StreamPhysicalWindowRank) node, requireDeterminism);
        } else if (node instanceof StreamPhysicalWindowTableFunction) {
            return matches((StreamPhysicalWindowTableFunction) node, requireDeterminism);
        } else if (node instanceof StreamPhysicalChangelogNormalize
                || node instanceof StreamPhysicalDropUpdateBefore
                || node instanceof StreamPhysicalMiniBatchAssigner
                || node instanceof StreamPhysicalUnion
                || node instanceof StreamPhysicalSort
                || node instanceof StreamPhysicalLimit
                || node instanceof StreamPhysicalSortLimit
                || node instanceof StreamPhysicalTemporalSort
                || node instanceof StreamPhysicalWatermarkAssigner
                || node instanceof StreamPhysicalExchange) {
            return matchChildren(node, requireDeterminism);
        }
        return Optional.empty();
    }

    private Optional<Tuple2<Integer, String>> matchChildren(
            RelNode rel, ImmutableBitSet requireDeterminism) {
        for (RelNode input : rel.getInputs()) {
            Optional<Tuple2<Integer, String>> matchInfo = matches(input, requireDeterminism);
            if (matchInfo.isPresent()) {
                return matchInfo;
            }
        }
        return Optional.empty();
    }

    private Optional<Tuple2<Integer, String>> matches(StreamPhysicalSink sink) {
        ImmutableBitSet requireDeterminism;
        if (ChangelogPlanUtils.inputInsertOnly(sink)) {
            requireDeterminism = ImmutableBitSet.of();
        } else {
            int[] primaryKey =
                    sink.contextResolvedTable().getResolvedSchema().getPrimaryKeyIndexes();

            if (sink.upsertMaterialize() || null == primaryKey || primaryKey.length == 0) {
                requireDeterminism =
                        ImmutableBitSet.range(sink.getInput().getRowType().getFieldCount());
            } else {
                requireDeterminism = ImmutableBitSet.of(primaryKey);
            }
        }
        return matchChildren(sink, requireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matches(StreamPhysicalLegacySink<?> sink) {
        ImmutableBitSet requireDeterminism;
        if (ChangelogPlanUtils.inputInsertOnly(sink)) {
            requireDeterminism = ImmutableBitSet.of();
        } else {
            TableSchema tableSchema = sink.sink().getTableSchema();
            Optional<UniqueConstraint> primaryKey = tableSchema.getPrimaryKey();
            List<String> columns = Arrays.asList(tableSchema.getFieldNames());

            if (primaryKey.isPresent()) {
                requireDeterminism =
                        ImmutableBitSet.of(
                                primaryKey.get().getColumns().stream()
                                        .map(columns::indexOf)
                                        .collect(Collectors.toList()));
            } else {
                requireDeterminism = ImmutableBitSet.range(columns.size());
            }
        }
        return matchChildren(sink, requireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matches(
            StreamPhysicalCalcBase calc, ImmutableBitSet requireDeterminism) {
        if (ChangelogPlanUtils.inputInsertOnly(calc) || requireDeterminism.isEmpty()) {
            requireDeterminism = ImmutableBitSet.of();
        } else {
            // if input has updates, any non-deterministic conditions are not acceptable, also
            // requireDeterminism should be satisfied.
            Optional<Tuple2<Integer, String>> matchInfo =
                    matchNonDeterministicRexProgram(requireDeterminism, calc.getProgram(), calc);
            if (matchInfo.isPresent()) {
                return matchInfo;
            }

            // evaluate required determinism from input
            List<RexNode> projects =
                    calc.getProgram().getProjectList().stream()
                            .map(expr -> calc.getProgram().expandLocalRef(expr))
                            .collect(Collectors.toList());
            Map<Integer, Integer> outFromSourcePos = extractSourceMapping(projects);
            List<Integer> conv2Inputs =
                    requireDeterminism.toList().stream()
                            .map(
                                    out ->
                                            Optional.ofNullable(outFromSourcePos.get(out))
                                                    .orElseThrow(
                                                            () ->
                                                                    new TableException(
                                                                            String.format(
                                                                                    "Invalid pos:%d over projection:%s",
                                                                                    out,
                                                                                    calc
                                                                                            .getProgram()))))
                            .filter(index -> index != -1)
                            .collect(Collectors.toList());
            requireDeterminism = ImmutableBitSet.of(conv2Inputs);
        }
        return matchChildren(calc, requireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matches(
            StreamPhysicalCorrelateBase correlate, ImmutableBitSet requireDeterminism) {
        if (ChangelogPlanUtils.inputInsertOnly(correlate) || requireDeterminism.isEmpty()) {
            requireDeterminism = ImmutableBitSet.of();
        }
        if (correlate.condition().isDefined()) {
            RexNode rexNode = correlate.condition().get();
            Optional<String> matchInfo = matchNonDeterministicCondition(rexNode);
            if (matchInfo.isPresent()) {
                return Optional.of(Tuple2.of(correlate.getId(), matchInfo.get()));
            }
        } else {
            // check if it is a non-deterministic function
            int leftFieldCnt = correlate.inputRel().getRowType().getFieldCount();
            Optional<String> ndCall =
                    FlinkRexUtil.getNonDeterministicCallNameInStreaming(correlate.scan().getCall());
            if (ndCall.isPresent()) {
                // all columns from table function scan cannot satisfy the required determinism
                List<Integer> unsatisfiedColumns =
                        requireDeterminism.toList().stream()
                                .filter(index -> index >= leftFieldCnt)
                                .collect(Collectors.toList());
                if (!unsatisfiedColumns.isEmpty()) {
                    return Optional.of(
                            Tuple2.of(
                                    correlate.getId(),
                                    matchNonDeterministicColumnsError(
                                            unsatisfiedColumns,
                                            correlate.getRowType(),
                                            null,
                                            ndCall)));
                }
            }
            // evaluate required determinism from input
            List<Integer> fromLeft =
                    requireDeterminism.toList().stream()
                            .filter(index -> index < leftFieldCnt)
                            .collect(Collectors.toList());
            requireDeterminism = ImmutableBitSet.of(fromLeft);
        }
        return matchChildren(correlate, requireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matches(
            StreamPhysicalLookupJoin lookupJoin, ImmutableBitSet requireDeterminism) {
        if (ChangelogPlanUtils.inputInsertOnly(lookupJoin) || requireDeterminism.isEmpty()) {
            requireDeterminism = ImmutableBitSet.of();
        } else {
            // required determinism cannot be satisfied even upsert materialize was enabled if:
            // 1. remaining join condition contains non-deterministic call
            Optional<RexNode> remainingCondition =
                    JavaScalaConversionUtil.toJava(lookupJoin.remainingCondition());
            if (remainingCondition.isPresent()) {
                Optional<String> matchInfo =
                        matchNonDeterministicCondition(remainingCondition.get());
                if (matchInfo.isPresent()) {
                    return Optional.of(Tuple2.of(lookupJoin.getId(), matchInfo.get()));
                }
            }

            // 2. inner calc in lookJoin contains either non-deterministic condition or calls
            Optional<RexProgram> calc =
                    JavaScalaConversionUtil.toJava(lookupJoin.calcOnTemporalTable());
            if (calc.isPresent()) {
                Optional<Tuple2<Integer, String>> matchInfo =
                        matchNonDeterministicRexProgram(requireDeterminism, calc.get(), lookupJoin);
                if (matchInfo.isPresent()) {
                    return matchInfo;
                }
            }
            // Try to resolve non-determinism by adding materialization which can eliminate
            // non-determinism produced by lookup join via an evolving source.
            int leftFieldCnt = lookupJoin.getInput().getRowType().getFieldCount();
            List<Integer> requireRight =
                    requireDeterminism.toList().stream()
                            .filter(index -> index >= leftFieldCnt)
                            .collect(Collectors.toList());
            boolean requireUpsertMaterialize = true;
            // two optimizations: 1. no fields from lookup source was required 2. lookup key
            // contains pk and no requirement on other fields we can omit materialization,
            // otherwise upsert materialize can not be omitted.
            if (requireRight.isEmpty()) {
                requireUpsertMaterialize = false;
            } else {
                int[] outputPkIdx = lookupJoin.getOutputIndexesOfTemporalTablePrimaryKey();
                ImmutableBitSet outputPkBitSet = ImmutableBitSet.of(outputPkIdx);
                // outputPkIdx need to used so not using #lookupKeyContainsPrimaryKey directly.
                requireUpsertMaterialize =
                        Arrays.stream(outputPkIdx)
                                        .allMatch(
                                                index -> lookupJoin.allLookupKeys().contains(index))
                                && requireRight.stream().allMatch(outputPkBitSet::get);
            }
            List<Integer> requireLeft =
                    requireDeterminism.toList().stream()
                            .filter(index -> index < leftFieldCnt)
                            .collect(Collectors.toList());
            if (requireUpsertMaterialize) {
                return Optional.of(
                        Tuple2.of(
                                lookupJoin.getId(),
                                "You might want to set 'table.exec.sink.upsert-materialize' = 'force' "
                                        + "to avoid data correctness risk caused by the this operator."));
            }
            requireDeterminism = ImmutableBitSet.of(requireLeft);
        }
        return matchChildren(lookupJoin, requireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matches(
            StreamPhysicalTableSourceScan tableScan, ImmutableBitSet requireDeterminism) {
        if (!requireDeterminism.isEmpty()) {
            boolean insertOnly =
                    tableScan.tableSource().getChangelogMode().containsOnly(RowKind.INSERT);
            boolean supportsReadingMetadata =
                    tableScan.tableSource() instanceof SupportsReadingMetadata;
            if (!insertOnly && supportsReadingMetadata) {
                TableSourceTable sourceTable = tableScan.getTable().unwrap(TableSourceTable.class);
                // check if requireDeterminism contains metadata column
                List<Column.MetadataColumn> metadataColumns =
                        DynamicSourceUtils.extractMetadataColumns(
                                sourceTable.contextResolvedTable().getResolvedSchema());
                Set<String> metaColumnSet =
                        metadataColumns.stream().map(Column::getName).collect(Collectors.toSet());
                List<String> columns = tableScan.getRowType().getFieldNames();
                List<String> metadataCauseErr = new ArrayList<>();
                for (int index = 0; index < columns.size(); index++) {
                    String column = columns.get(index);
                    if (metaColumnSet.contains(column) && requireDeterminism.get(index)) {
                        metadataCauseErr.add(column);
                    }
                }
                if (!metadataCauseErr.isEmpty()) {
                    return Optional.of(
                            Tuple2.of(
                                    tableScan.getId(),
                                    "The metadata column(s): '"
                                            + String.join(
                                                    ", ", metadataCauseErr.toArray(new String[0]))
                                            + "' in cdc source may cause wrong result or error on"
                                            + " downstream operators, please consider removing these"
                                            + " columns or use a non-cdc source that only has insert"
                                            + " messages."));
                }
            }
        }
        return Optional.empty();
    }

    private Optional<Tuple2<Integer, String>> matches(
            StreamPhysicalGroupAggregateBase groupAgg, ImmutableBitSet requireDeterminism) {
        if (ChangelogPlanUtils.inputInsertOnly(groupAgg)) {
            if (!requireDeterminism.isEmpty()) {
                Optional<Tuple2<Integer, String>> matchInfo =
                        matchUnsatisfiedDeterminism(
                                requireDeterminism,
                                groupAgg.grouping().length,
                                // TODO remove this conversion when scala-free was total done.
                                scala.collection.JavaConverters.seqAsJavaList(groupAgg.aggCalls()),
                                groupAgg.getRowType(),
                                groupAgg);
                if (matchInfo.isPresent()) {
                    return matchInfo;
                }
                requireDeterminism = ImmutableBitSet.of();
            }
        } else {
            requireDeterminism =
                    ImmutableBitSet.range(groupAgg.getInput().getRowType().getFieldCount());
        }
        return matchChildren(groupAgg, requireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matches(
            StreamPhysicalWindowAggregateBase windowAgg, ImmutableBitSet requireDeterminism) {
        if (ChangelogPlanUtils.inputInsertOnly(windowAgg)) {
            // no further requirement to input, only check if it can satisfy the
            // requiredDeterminism
            if (!requireDeterminism.isEmpty()) {
                Optional<Tuple2<Integer, String>> matchInfo =
                        matchUnsatisfiedDeterminism(
                                requireDeterminism,
                                windowAgg.grouping().length,
                                // TODO remove this conversion when scala-free was total done.
                                scala.collection.JavaConverters.seqAsJavaList(windowAgg.aggCalls()),
                                windowAgg.getRowType(),
                                windowAgg);
                if (matchInfo.isPresent()) {
                    return matchInfo;
                }
            }
            requireDeterminism = ImmutableBitSet.of();
        } else {
            // agg works under retract mode if input is not insert only, and requires all input
            // columns be deterministic
            requireDeterminism =
                    ImmutableBitSet.range(windowAgg.getInput().getRowType().getFieldCount());
        }

        return matchChildren(windowAgg, requireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matches(
            StreamPhysicalExpand expand, ImmutableBitSet requireDeterminism) {
        return matchChildren(
                expand, requireDeterminism.except(ImmutableBitSet.of(expand.expandIdIndex())));
    }

    private Optional<Tuple2<Integer, String>> matches(
            CommonPhysicalJoin join, ImmutableBitSet requireDeterminism) {
        StreamPhysicalRel leftRel = (StreamPhysicalRel) join.getLeft();
        StreamPhysicalRel rightRel = (StreamPhysicalRel) join.getRight();
        boolean leftInputHasUpdate = !ChangelogPlanUtils.inputInsertOnly(leftRel);
        boolean rightInputHasUpdate = !ChangelogPlanUtils.inputInsertOnly(rightRel);
        boolean innerOrSemi =
                join.joinSpec().getJoinType() == FlinkJoinType.INNER
                        || join.joinSpec().getJoinType() == FlinkJoinType.SEMI;
        /**
         * we do not distinguish the time attribute condition in interval/temporal join from
         * regular/window join here because: rowtime field always from source, proctime is not
         * limited (from source), when proctime appended to an update row without upsertKey then
         * result may goes wrong, in such a case proctime( was materialized as
         * PROCTIME_MATERIALIZE(PROCTIME())) is equal to a normal dynamic temporal function and will
         * be validated in calc node.
         */
        Optional<String> ndCall =
                FlinkRexUtil.getNonDeterministicCallNameInStreaming(join.getCondition());
        if ((leftInputHasUpdate || rightInputHasUpdate || !innerOrSemi) && ndCall.isPresent()) {
            // when output has update, the join condition cannot be non-deterministic:
            // 1. input has update -> output has update
            // 2. input insert only and is not innerOrSemi join -> output has update
            return Optional.of(
                    Tuple2.of(
                            join.getId(),
                            matchNonDeterministicCondition(ndCall.get(), join.getCondition())));
        }
        int leftFieldCnt = leftRel.getRowType().getFieldCount();
        Optional<Tuple2<Integer, String>> matchLeft =
                matchJoinChild(
                        requireDeterminism,
                        leftRel,
                        leftInputHasUpdate,
                        leftFieldCnt,
                        true,
                        join.joinSpec().getLeftKeys(),
                        // TODO remove this conversion when scala-free was total done.
                        scala.collection.JavaConverters.seqAsJavaList(
                                join.getUpsertKeys(leftRel, join.joinSpec().getLeftKeys())));
        if (matchLeft.isPresent()) {
            return matchLeft;
        }
        return matchJoinChild(
                requireDeterminism,
                rightRel,
                rightInputHasUpdate,
                leftFieldCnt,
                false,
                join.joinSpec().getRightKeys(),
                // TODO remove this conversion when scala-free was total done.
                scala.collection.JavaConverters.seqAsJavaList(
                        join.getUpsertKeys(rightRel, join.joinSpec().getRightKeys())));
    }

    private Optional<Tuple2<Integer, String>> matchJoinChild(
            final ImmutableBitSet requireDeterminism,
            final StreamPhysicalRel rel,
            final boolean inputHasUpdate,
            final int leftFieldCnt,
            final boolean isLeft,
            final int[] joinKeys,
            final List<int[]> inputUniqueKeys) {
        JoinInputSideSpec joinInputSideSpec =
                JoinUtil.analyzeJoinInput(
                        ShortcutUtils.unwrapClassLoader(rel),
                        InternalTypeInfo.of(FlinkTypeFactory.toLogicalRowType(rel.getRowType())),
                        joinKeys,
                        inputUniqueKeys);
        ImmutableBitSet inputRequireDeterminism;
        if (inputHasUpdate) {
            if (joinInputSideSpec.hasUniqueKey() || joinInputSideSpec.joinKeyContainsUniqueKey()) {
                // join hasUniqueKey or joinKeyContainsUniqueKey, then transmit corresponding
                // requirement to input
                if (isLeft) {
                    inputRequireDeterminism =
                            ImmutableBitSet.of(
                                    requireDeterminism.toList().stream()
                                            .filter(index -> index < leftFieldCnt)
                                            .collect(Collectors.toList()));
                } else {
                    inputRequireDeterminism =
                            ImmutableBitSet.of(
                                    requireDeterminism.toList().stream()
                                            .filter(index -> index >= leftFieldCnt)
                                            .map(index -> index - leftFieldCnt)
                                            .collect(Collectors.toList()));
                }
            } else {
                // join need to retract by whole input row
                inputRequireDeterminism = ImmutableBitSet.range(rel.getRowType().getFieldCount());
            }
        } else {
            inputRequireDeterminism = ImmutableBitSet.of();
        }
        return matchChildren(rel, inputRequireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matches(
            StreamPhysicalOverAggregateBase overAgg, ImmutableBitSet requireDeterminism) {
        if (ChangelogPlanUtils.inputInsertOnly(overAgg)) {
            // no further requirement to input, only check if the agg outputs can satisfy the
            // requiredDeterminism
            if (!requireDeterminism.isEmpty()) {
                int inputFieldCnt = overAgg.getInput().getRowType().getFieldCount();
                OverSpec overSpec = OverAggregateUtil.createOverSpec(overAgg.logicWindow());
                // add aggCall's input
                int aggOutputIndex = inputFieldCnt;
                for (OverSpec.GroupSpec groupSpec : overSpec.getGroups()) {
                    Optional<Tuple2<Integer, String>> matchInfo =
                            matchUnsatisfiedDeterminism(
                                    requireDeterminism,
                                    aggOutputIndex,
                                    groupSpec.getAggCalls(),
                                    overAgg.getRowType(),
                                    overAgg);
                    if (matchInfo.isPresent()) {
                        return matchInfo;
                    }
                    aggOutputIndex += groupSpec.getAggCalls().size();
                }
            }
            requireDeterminism = ImmutableBitSet.of();
        } else {
            // OverAgg does not support input with updates currently, so this branch will not be
            // reached for now.

            // We should append partition keys and order key to requireDeterminism
            requireDeterminism = mappingRequireDeterminismToInput(requireDeterminism, overAgg);
        }
        return matchChildren(overAgg, requireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matches(
            StreamPhysicalRank rank, ImmutableBitSet requireDeterminism) {
        if (ChangelogPlanUtils.inputInsertOnly(rank)) {
            // rank output is deterministic when input is insert only, so required determinism
            // always be satisfied here.
            requireDeterminism = ImmutableBitSet.of();
        } else {
            int inputFieldCnt = rank.getInput().getRowType().getFieldCount();
            if (rank.rankStrategy() instanceof RankProcessStrategy.UpdateFastStrategy) {
                // in update fast mode, pass required determinism excludes partition keys and
                // order key
                ImmutableBitSet.Builder bitSetBuilder = ImmutableBitSet.builder();
                rank.partitionKey().toList().forEach(bitSetBuilder::set);
                rank.orderKey().getKeys().toIntegerList().forEach(bitSetBuilder::set);
                if (rank.outputRankNumber()) {
                    // exclude last column
                    bitSetBuilder.set(inputFieldCnt);
                }
                requireDeterminism = requireDeterminism.except(bitSetBuilder.build());
            } else if (rank.rankStrategy() instanceof RankProcessStrategy.RetractStrategy) {
                // in retract mode then require all input columns be deterministic
                requireDeterminism = ImmutableBitSet.range(inputFieldCnt);
            } else {
                // AppendFastStrategy only applicable for insert only input, so the undefined
                // strategy is not as expected here.
                return Optional.of(
                        Tuple2.of(
                                rank.getId(),
                                String.format(
                                        "Can not infer the determinism for unsupported rank strategy: %s, this is a bug, please file an issue.",
                                        rank.rankStrategy())));
            }
        }
        return matchChildren(rank, requireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matches(
            StreamPhysicalDeduplicate dedup, ImmutableBitSet requireDeterminism) {
        if (ChangelogPlanUtils.inputInsertOnly(dedup)) {
            // similar to rank, output is deterministic when input is insert only, so required
            // determinism always be satisfied here.
            requireDeterminism = ImmutableBitSet.of();
        } else {
            // Deduplicate always has unique key currently(exec node has null check and inner
            // state only support data with keys), so only pass the left columns of required
            // determinism to input.
            requireDeterminism =
                    requireDeterminism.except(ImmutableBitSet.of(dedup.getUniqueKeys()));
        }
        return matchChildren(dedup, requireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matches(
            StreamPhysicalWindowDeduplicate windowDedup, ImmutableBitSet requireDeterminism) {
        if (ChangelogPlanUtils.inputInsertOnly(windowDedup)) {
            // similar to rank, output is deterministic when input is insert only, so required
            // determinism always be satisfied here.
            requireDeterminism = ImmutableBitSet.of();
        } else {
            // WindowDeduplicate does not support input with updates currently, so this branch
            // will not be reached for now.

            // only append partition keys, no need to process order key because it always comes
            // from window
            requireDeterminism =
                    requireDeterminism
                            .clear(windowDedup.orderKey())
                            .union(ImmutableBitSet.of(windowDedup.partitionKeys()));
        }
        return matchChildren(windowDedup, requireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matches(
            StreamPhysicalWindowRank windowRank, ImmutableBitSet requireDeterminism) {
        if (ChangelogPlanUtils.inputInsertOnly(windowRank)) {
            // similar to rank, output is deterministic when input is insert only, so required
            // determinism always be satisfied here.
            requireDeterminism = ImmutableBitSet.of();
        } else {
            // WindowRank does not support input with updates currently, so this branch will not
            // be reached for now.

            // only append partition keys, no need to process order key because it always comes
            // from window
            int inputFieldCnt = windowRank.getInput().getRowType().getFieldCount();
            requireDeterminism =
                    requireDeterminism
                            .intersect(ImmutableBitSet.range(inputFieldCnt))
                            .union(windowRank.partitionKey());
        }
        return matchChildren(windowRank, requireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matches(
            StreamPhysicalWindowTableFunction windowTVF, ImmutableBitSet requireDeterminism) {
        if (ChangelogPlanUtils.inputInsertOnly(windowTVF)) {
            requireDeterminism = ImmutableBitSet.of();
        } else {
            // pass the left columns of required determinism to input exclude window attributes
            requireDeterminism =
                    requireDeterminism.intersect(
                            ImmutableBitSet.range(
                                    windowTVF.getInput().getRowType().getFieldCount()));
        }
        return matchChildren(windowTVF, requireDeterminism);
    }

    private Optional<Tuple2<Integer, String>> matchNonDeterministicRexProgram(
            final ImmutableBitSet requireDeterminism,
            final RexProgram program,
            final StreamPhysicalRel relatedRel) {
        if (null != program.getCondition()) {
            // firstly check if exists non-deterministic condition
            RexNode rexNode = program.expandLocalRef(program.getCondition());
            Optional<String> matchInfo = matchNonDeterministicCondition(rexNode);
            if (matchInfo.isPresent()) {
                return Optional.of(Tuple2.of(relatedRel.getId(), matchInfo.get()));
            }
        }
        // extract all non-deterministic output columns first and check if any of them were
        // required be deterministic.
        List<RexNode> projects =
                program.getProjectList().stream()
                        .map(program::expandLocalRef)
                        .collect(Collectors.toList());
        Map<Integer, String> nonDeterministicCols = new HashMap<>();
        for (int index = 0; index < projects.size(); index++) {
            Optional<String> ndCall =
                    FlinkRexUtil.getNonDeterministicCallNameInStreaming(projects.get(index));
            if (ndCall.isPresent()) {
                nonDeterministicCols.put(index, ndCall.get());
            } // else ignore
        }
        List<Integer> unsatisfiedColumns =
                requireDeterminism.toList().stream()
                        .filter(nonDeterministicCols::containsKey)
                        .collect(Collectors.toList());
        if (!unsatisfiedColumns.isEmpty()) {
            return Optional.of(
                    Tuple2.of(
                            relatedRel.getId(),
                            matchNonDeterministicColumnsError(
                                    unsatisfiedColumns,
                                    relatedRel.getRowType(),
                                    nonDeterministicCols,
                                    Optional.empty())));
        }
        return Optional.empty();
    }

    private String matchNonDeterministicColumnsError(
            final List<Integer> indexes,
            final RelDataType rowType,
            final Map<Integer, String> ndCallMap,
            final Optional<String> ndCallName) {
        StringBuilder errorMsg = new StringBuilder();
        errorMsg.append("The column(s): ");
        int index = 0;
        for (String column : rowType.getFieldNames()) {
            if (indexes.contains(index)) {
                errorMsg.append(column).append("(generated by non-deterministic function: ");
                if (ndCallName.isPresent()) {
                    errorMsg.append(ndCallName.get());
                } else {
                    errorMsg.append(ndCallMap.get(index));
                }
                errorMsg.append(" ) ");
            }
            index++;
        }
        errorMsg.append(
                "can not satisfy the determinism requirement for correctly processing update message("
                        + "'UB'/'UA'/'D' in changelogMode, not 'I' only), this usually happens when input node has"
                        + " no upsertKey(upsertKeys=[{}]) or current node outputs non-deterministic update "
                        + "messages. Please consider removing these non-deterministic columns or making them "
                        + "deterministic by using deterministic functions.\n");
        return errorMsg.toString();
    }

    private Optional<String> matchNonDeterministicCondition(final RexNode condition) {
        Optional<String> ndCall = FlinkRexUtil.getNonDeterministicCallNameInStreaming(condition);
        return ndCall.map(s -> matchNonDeterministicCondition(s, condition));
    }

    private String matchNonDeterministicCondition(String ndCall, RexNode condition) {
        return String.format(
                "There exists non deterministic function: '%s' in condition: '%s' which may cause wrong result in update pipeline.",
                ndCall, condition);
    }

    private Optional<Tuple2<Integer, String>> matchUnsatisfiedDeterminism(
            final ImmutableBitSet requireDeterminism,
            final int aggStartIndex,
            final List<AggregateCall> aggCalls,
            final RelDataType rowType,
            final StreamPhysicalRel relatedRel) {
        Map<Integer, String> nonDeterministicOutput = new HashMap<>();
        // skip checking non-deterministic columns in grouping keys or filter args in agg call
        // because they were pushed down to input project which processes input only message
        int aggOutputIndex = aggStartIndex;
        for (AggregateCall aggCall : aggCalls) {
            if (!aggCall.getAggregation().isDeterministic()
                    || aggCall.getAggregation().isDynamicFunction()) {
                nonDeterministicOutput.put(aggOutputIndex, aggCall.getAggregation().getName());
            }
            aggOutputIndex++;
        }
        // check if exist non-deterministic aggCalls which were in requireDeterminism
        List<Integer> unsatisfiedColumns =
                requireDeterminism.toList().stream()
                        .filter(nonDeterministicOutput::containsKey)
                        .collect(Collectors.toList());
        if (!unsatisfiedColumns.isEmpty()) {
            return Optional.of(
                    Tuple2.of(
                            relatedRel.getId(),
                            matchNonDeterministicColumnsError(
                                    unsatisfiedColumns,
                                    rowType,
                                    nonDeterministicOutput,
                                    Optional.empty())));
        }
        return Optional.empty();
    }

    /** Extracts the out from source field index mapping of the given projects. */
    private Map<Integer, Integer> extractSourceMapping(final List<RexNode> projects) {
        Map<Integer, Integer> mapOutFromInPos = new HashMap<>();

        for (int index = 0; index < projects.size(); index++) {
            RexNode expr = projects.get(index);
            if (expr instanceof RexInputRef) {
                mapOutFromInPos.put(index, ((RexInputRef) expr).getIndex());
            } else if (expr instanceof RexCall) {
                // rename or cast call
                RexCall call = (RexCall) expr;
                if ((call.getKind().equals(SqlKind.AS) || call.getKind().equals(SqlKind.CAST))
                        && (call.getOperands().get(0) instanceof RexInputRef)) {
                    RexInputRef ref = (RexInputRef) call.getOperands().get(0);
                    mapOutFromInPos.put(index, ref.getIndex());
                }
            } else if (expr instanceof RexLiteral) {
                mapOutFromInPos.put(index, -1);
            }
            // else ignore
        }

        return mapOutFromInPos;
    }

    private ImmutableBitSet mappingRequireDeterminismToInput(
            final ImmutableBitSet requireDeterminism,
            final StreamPhysicalOverAggregateBase overAgg) {
        int inputFieldCnt = overAgg.getInput().getRowType().getFieldCount();
        List<Integer> requireInputIndexes =
                requireDeterminism.toList().stream()
                        .filter(index -> index < inputFieldCnt)
                        .collect(Collectors.toList());
        if (requireInputIndexes.size() == inputFieldCnt) {
            return ImmutableBitSet.range(inputFieldCnt);
        } else {
            Set<Integer> allRequiredInputSet = new HashSet<>(requireInputIndexes);

            OverSpec overSpec = OverAggregateUtil.createOverSpec(overAgg.logicWindow());
            // add partitionKeys
            Arrays.stream(overSpec.getPartition().getFieldIndices())
                    .forEach(allRequiredInputSet::add);
            // add aggCall's input
            overSpec.getGroups().forEach(OverSpec.GroupSpec::getAggCalls);
            int aggOutputIndex = inputFieldCnt;
            for (OverSpec.GroupSpec groupSpec : overSpec.getGroups()) {
                for (AggregateCall aggCall : groupSpec.getAggCalls()) {
                    if (requireDeterminism.get(aggOutputIndex)) {
                        requiredSourceInput(aggCall, allRequiredInputSet);
                    }
                    aggOutputIndex++;
                }
            }
            assert allRequiredInputSet.size() <= inputFieldCnt;
            return ImmutableBitSet.of(new ArrayList<>(allRequiredInputSet));
        }
    }

    private void requiredSourceInput(
            final AggregateCall aggCall, final Set<Integer> requiredInputSet) {
        // add agg args first
        requiredInputSet.addAll(aggCall.getArgList());
        // add agg filter args
        if (aggCall.filterArg > -1) {
            requiredInputSet.add(aggCall.filterArg);
        }
    }
}
