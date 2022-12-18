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

    private String content;
    private final AdviceKind kind;

    public PlanAdvice(String advisor, String content, AdviceKind kind) {
        this.content = content;
        this.kind = kind;
    }

    public PlanAdvice(String advisor, AdviceKind kind) {
        this(advisor, "", kind);
    }

    public void withContent(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    public AdviceKind getKind() {
        return kind;
    }

    /** Categorizes the semantics of a {@link PlanAdvice}. */
    @Experimental
    public enum AdviceKind {
        /** Indicate the potential risk. */
        WARNING,
        /** Indicate the potential optimization. */
        HINT
    }
}
