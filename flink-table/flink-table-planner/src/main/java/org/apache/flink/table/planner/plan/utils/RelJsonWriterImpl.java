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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.planner.analyze.PlanAdvice;
import org.apache.flink.table.planner.analyze.PlanAnalyzer;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.calcite.plan.RelOptNode;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Serialize the optimized physical plan to graph-style JSON for explain statement with explain
 * detail as {@code org.apache.flink.table.api.ExplainDetail.ANALYZED_PHYSICAL_PLAN}, with available
 * advices.
 */
public class RelJsonWriterImpl extends RelWriterImpl {

    private static final String ADVICE = "advice";
    private static final String CONTENT = "content";
    private static final String CHANGELOG_MODE = "changelog_mode";
    private static final String DISTRIBUTION = "distribution";
    private static final String DIGEST = "digest";
    private static final String ID = "id";
    private static final String KIND = "kind";
    private static final String NODES = "nodes";
    private static final String TYPE = "type";
    private static final String PREDECESSORS = "predecessors";
    private static final String SCOPE = "scope";

    private final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();
    private final List<Vertex> vertices = new ArrayList<>();
    private final Map<Integer, Edge> relIdToOutputEdge = new HashMap<>();

    private final Map<Integer, Integer> relIdToVertex = new HashMap<>();

    /** For plan reuse, map key is the reused rel id. */
    private final Map<Integer, Vertex> reusedRelIdToVertex = new HashMap<>();

    private final AtomicInteger counter = new AtomicInteger(1);

    private final List<PlanAnalyzer.AnalyzedResult> analyzedResults;

    private TableConfig tableConfig;

    private int reuseVertexId;

    private boolean latestVertexIsReused;

    public RelJsonWriterImpl(PrintWriter pw, List<PlanAnalyzer.AnalyzedResult> analyzedResults) {
        super(pw, SqlExplainLevel.DIGEST_ATTRIBUTES, false);
        this.analyzedResults = analyzedResults;
    }

    @Override
    protected void explain_(RelNode rel, List<Pair<String, @Nullable Object>> values) {
        if (tableConfig == null) {
            tableConfig = ShortcutUtils.unwrapTableConfig(rel);
        }
        visit(rel);
    }

    public void printToJson() {
        pw.print(toJson());
    }

    public void addResults(List<PlanAnalyzer.AnalyzedResult> results) {
        this.analyzedResults.addAll(results);
    }

    private String toJson() {
        ObjectNode json = mapper.createObjectNode();
        ArrayNode nodeList = mapper.createArrayNode();
        json.put(NODES, nodeList);
        for (Vertex vertex : vertices) {
            ObjectNode node = mapper.createObjectNode();
            node.put(ID, vertex.id);
            node.put(TYPE, vertex.type);
            node.put(DIGEST, vertex.digest);
            node.put(CHANGELOG_MODE, vertex.changelogMode);
            if (!vertex.predecessors.isEmpty()) {
                ArrayNode edgeList = mapper.createArrayNode();
                node.put(PREDECESSORS, edgeList);
                for (Edge predecessor : vertex.predecessors) {
                    ObjectNode edge = mapper.createObjectNode();
                    edge.put(ID, predecessor.id);
                    edge.put(DISTRIBUTION, predecessor.distribution);
                    edge.put(CHANGELOG_MODE, predecessor.changelogMode);
                    edgeList.add(edge);
                }
            }
            nodeList.add(node);
        }
        if (!analyzedResults.isEmpty()) {
            ArrayNode adviceList = mapper.createArrayNode();
            json.put(ADVICE, adviceList);
            for (PlanAnalyzer.AnalyzedResult result : analyzedResults) {
                ObjectNode advice = mapper.createObjectNode();
                advice.put(KIND, result.getAdvice().getKind().name());
                advice.put(SCOPE, result.getAdvice().getScope().name());
                advice.put(CONTENT, result.getAdvice().getContent());
                if (result.getAdvice().getScope() == PlanAdvice.Scope.LOCAL) {
                    ArrayNode vertexList = mapper.createArrayNode();
                    advice.put(ID, vertexList);
                    result.getTargetIds().stream()
                            .map(relIdToVertex::get)
                            .sorted()
                            .forEach(vertexList::add);
                }
                adviceList.add(advice);
            }
        }
        return json.toPrettyString();
    }

    private void visit(RelNode rel) {
        for (RelNode child : rel.getInputs()) {
            visit(child);
        }
        // create edge instead of vertex for exchange
        if (rel instanceof Exchange) {
            int prevId = latestVertexIsReused ? reuseVertexId : counter.get() - 1;
            if (!latestVertexIsReused) {
                relIdToOutputEdge.values().stream()
                        .filter(edge -> edge.id == prevId)
                        .findAny()
                        .ifPresent(
                                edge -> {
                                    edge.distribution =
                                            ((Exchange) rel).getDistribution().getType().name();
                                    edge.changelogMode = getChangelogMode(rel);
                                    relIdToOutputEdge.put(rel.getId(), edge);
                                });
                counter.getAndIncrement();
            }
        } else {
            Vertex current = createVertexIfNotExist(rel);
            if (!latestVertexIsReused) {
                rel.getInputs().stream()
                        .map(RelOptNode::getId)
                        .map(relIdToOutputEdge::get)
                        .forEach(current::addPredecessor);
                vertices.add(current);
                relIdToOutputEdge.put(rel.getId(), createEdge(current.id, current.changelogMode));
                counter.getAndIncrement();
                relIdToVertex.put(rel.getId(), current.id);
            } else {
                reuseVertexId = current.id;
            }
        }
    }

    private Vertex createVertexIfNotExist(RelNode rel) {
        boolean tableSourceReuseEnabled =
                tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED);
        if (tableSourceReuseEnabled) {
            Vertex vertex = reusedRelIdToVertex.get(rel.getId());
            if (vertex == null) {
                latestVertexIsReused = false;
                vertex = createVertex(rel);
                reusedRelIdToVertex.put(rel.getId(), vertex);
            } else {
                latestVertexIsReused = true;
            }
            return vertex;
        } else {
            latestVertexIsReused = false;
            return createVertex(rel);
        }
    }

    private Vertex createVertex(RelNode rel) {
        TableConfigOptions.CatalogPlanCompilation compilation =
                tableConfig.get(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS);
        SqlExplainLevel level = SqlExplainLevel.NO_ATTRIBUTES;
        boolean withRowType = false;
        boolean withUpsertKey = false;
        if (compilation != TableConfigOptions.CatalogPlanCompilation.IDENTIFIER) {
            level = SqlExplainLevel.DIGEST_ATTRIBUTES;
            withUpsertKey = true;
        }
        if (compilation == TableConfigOptions.CatalogPlanCompilation.ALL) {
            withRowType = true;
        }
        return new Vertex(
                counter.get(),
                rel.getRelTypeName(),
                FlinkRelOptUtil.toString(
                                rel, level, false, false, withRowType, withUpsertKey, false)
                        .split("\\n")[0],
                getChangelogMode(rel));
    }

    private String getChangelogMode(RelNode rel) {
        if (rel instanceof StreamPhysicalRel) {
            return ChangelogPlanUtils.stringifyChangelogMode(
                    ChangelogPlanUtils.getChangelogMode((StreamPhysicalRel) rel));
        }
        return "None";
    }

    private Edge createEdge(int id, String changelogMode) {
        return new Edge(id, RelDistribution.Type.ANY.name(), changelogMode);
    }

    private static class Vertex {

        private final int id;
        private final String type;
        private final String digest;
        private final String changelogMode;
        private final List<Edge> predecessors = new ArrayList<>();

        private Vertex(int id, String type, String digest, String changelogMode) {
            this.id = id;
            this.type = type;
            this.digest = digest;
            this.changelogMode = changelogMode;
        }

        void addPredecessor(Edge edge) {
            predecessors.add(edge);
        }
    }

    private static class Edge {
        private final int id;
        private String distribution;
        private String changelogMode;

        private Edge(int id, String distribution, String changelogMode) {
            this.id = id;
            this.distribution = distribution;
            this.changelogMode = changelogMode;
        }
    }
}
