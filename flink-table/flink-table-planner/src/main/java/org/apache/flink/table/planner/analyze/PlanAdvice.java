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

/** Plain POJO for advice provided by {@link PlanAnalyzer}. */
@Experimental
public final class PlanAdvice {

    private final Kind kind;

    private final Scope scope;

    private String content;

    public PlanAdvice(Kind kind, Scope scope, String content) {
        this.kind = kind;
        this.scope = scope;
        this.content = content;
    }

    public PlanAdvice(Kind kind, Scope scope) {
        this(kind, scope, "");
    }

    public PlanAdvice withContent(String content) {
        this.content = content;
        return this;
    }

    public String getContent() {
        return content;
    }

    public Scope getScope() {
        return scope;
    }

    public Kind getKind() {
        return kind;
    }

    /** Categorize the semantics of a {@link PlanAdvice}. */
    @Experimental
    public enum Kind {
        /** Indicate the potential risk. */
        WARNING,
        /** Indicate the potential optimization. */
        ADVICE
    }

    /** Categorize the scope of a {@link PlanAdvice}. */
    @Experimental
    public enum Scope {
        /**
         * Indicate a global advice, which is not specific to a {@link
         * org.apache.calcite.rel.RelNode}.
         */
        GLOBAL,
        /**
         * Indicate a local advice, which could be located to a specific {@link
         * org.apache.calcite.rel.RelNode}.
         */
        LOCAL
    }
}
