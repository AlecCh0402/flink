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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Poc version of json serialization for {@code
 * org.apache.flink.table.api.ExplainDetail.ANALYZED_PHYSICAL_PLAN}.
 */
public class RelJsonWriterImpl extends RelWriterImpl {

    public static final String ADVICE = "advice";
    public static final String CONTENT = "content";
    public static final String CHANGELOG_MODE = "changelog_mode";
    public static final String DISTRIBUTION = "distribution";
    public static final String DIGEST = "digest";
    public static final String ID = "id";
    public static final String NODES = "nodes";
    public static final String TYPE = "type";
    public static final String PREDECESSORS = "predecessors";
    public static final String SCOPE = "scope";

    private final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();
    private final List<Vertex> vertices = new ArrayList<>();
    private final Map<Integer, Edge> relIdToOutputEdge = new HashMap<>();

    private final Map<Integer, Integer> relIdToVertex = new HashMap<>();

    /** For plan reuse. */
    private final Map<Integer, Vertex> reusedRelIdToVertex = new HashMap<>();

    private final AtomicInteger counter = new AtomicInteger(1);

    /** For statement set. */
    private final boolean singleRel;

    private final List<PlanAnalyzer.AnalyzedResult> analyzedResults;

    private int reuseVertexId;

    private boolean latestVertexIsReused;

    public RelJsonWriterImpl(
            PrintWriter pw, List<PlanAnalyzer.AnalyzedResult> analyzedResults, boolean singleRel) {
        super(pw, SqlExplainLevel.DIGEST_ATTRIBUTES, false);
        this.analyzedResults = analyzedResults;
        this.singleRel = singleRel;
    }

    @Override
    protected void explain_(RelNode rel, List<Pair<String, @Nullable Object>> values) {
        visit(rel);
        if (singleRel) {
            pw.print(toJson());
        }
    }

    public void printToJson() {
        if (!singleRel) {
            pw.print(toJson());
        }
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
                advice.put(TYPE, result.getAdvice().getKind().name());
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
        TableConfig tableConfig = ShortcutUtils.unwrapTableConfig(rel);
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
        return new Vertex(
                counter.get(),
                rel.getRelTypeName(),
                FlinkRelOptUtil.toString(
                                rel,
                                SqlExplainLevel.DIGEST_ATTRIBUTES,
                                false,
                                true,
                                true,
                                true,
                                false)
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Vertex)) {
                return false;
            }
            Vertex vertex = (Vertex) o;
            return id == vertex.id
                    && type.equals(vertex.type)
                    && digest.equals(vertex.digest)
                    && changelogMode.equals(vertex.changelogMode)
                    && predecessors.equals(vertex.predecessors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, type, digest, changelogMode, predecessors);
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

        @Override
        public String toString() {
            return "Edge{"
                    + "id="
                    + id
                    + ", distribution='"
                    + distribution
                    + '\''
                    + ", changelogMode='"
                    + changelogMode
                    + '\''
                    + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Edge)) {
                return false;
            }
            Edge edge = (Edge) o;
            return id == edge.id
                    && distribution.equals(edge.distribution)
                    && changelogMode.equals(edge.changelogMode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, distribution, changelogMode);
        }
    }
}
