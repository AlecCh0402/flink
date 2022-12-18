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
package org.apache.flink.table.planner.analyze

import org.apache.flink.table.api.ExplainDetail
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation
import org.apache.flink.table.api.config.TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.planner.utils.TableTestUtil.{getPrettyJson, readFromResource}

import org.junit.{Before, Test}
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.util

/** Test for [[PlanAnalyzer]]. */
@RunWith(classOf[Parameterized])
class PlanAnalyzerTest(compilation: CatalogPlanCompilation)
  extends TableTestBase
  with Serializable {

  private val util = streamTestUtil()

  @Before
  def before(): Unit = {
    util.getPlanner.getTableConfig.set(PLAN_COMPILE_CATALOG_OBJECTS.key(), compilation.name())
    util.tableEnv.executeSql("""
                               |create table `order` (
                               |  order_id bigint not null primary key not enforced,
                               |  gmt_create timestamp(3) not null,
                               |  buyer_id bigint not null,
                               |  category_id bigint not null,
                               |  amount double not null,
                               |  ptime as proctime()
                               |) with (
                               |  'connector' = 'values',
                               |  'bounded' = 'false'
                               |)
    """.stripMargin)

    util.tableEnv.executeSql("""
                               |create table `category` (
                               |  category_id bigint not null primary key not enforced,
                               |  category_name string not null
                               |) with (
                               |  'connector' = 'values',
                               |  'bounded' = 'false'
                               |)
                               |""".stripMargin)
  }

  @Test
  def testExplainStateTTLSensitiveQuery(): Unit = {
    util.getPlanner.getTableConfig.set("table.exec.state.ttl", "3600")
    checkPlan(
      "testExplainStateTTLSensitiveQuery",
      """
        |select
        |  b.category_name,
        |  sum(a.amount) as revenue
        |from
        |  `order` a left join `category` for system_time as of `a`.`ptime` as `b`
        |on a.category_id = b.category_id
        |group by b.category_name""".stripMargin
    )
  }

  @Test
  def testExplainSplitDistinctAgg(): Unit = {
    checkPlan(
      "testExplainSplitDistinctAgg",
      """
        |select
        |  b.category_name,
        |  sum(a.amount) as revenue,
        |  count(distinct a.buyer_id) as buyer_cnt
        |from
        |  `order` a left join `category` for system_time as of `a`.`ptime` as `b`
        |on a.category_id = b.category_id
        |group by b.category_name""".stripMargin
    )
  }

  @Test
  def testExplainNonDeterministicUpdate(): Unit = {
    util.tableEnv.executeSql(
      """
        |create temporary table cdc_with_meta (
        | a int,
        | b bigint,
        | c string,
        | d boolean,
        | metadata_1 int metadata,
        | metadata_2 string metadata,
        | metadata_3 bigint metadata,
        | primary key (a) not enforced
        |) with (
        | 'connector' = 'values',
        | 'changelog-mode' = 'I,UA,UB,D',
        | 'readable-metadata' = 'metadata_1:INT, metadata_2:STRING, metadata_3:BIGINT'
        |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink_without_pk (
                               | a int,
                               | b bigint,
                               | c string
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)
    checkPlan(
      "testExplainNonDeterministicUpdate",
      s"""
         |insert into sink_without_pk
         |select a, metadata_3, c
         |from cdc_with_meta
         |""".stripMargin
    )
  }

  def checkPlan(testName: String, query: String): Unit = {
    val actual = util.tableEnv.explainSql(query, ExplainDetail.ANALYZED_PHYSICAL_PLAN)
    val expected = if (compilation == CatalogPlanCompilation.ALL) {
      readFromResource(s"explain/stream/analyzer/${testName}_ALL.out")
    } else if (compilation == CatalogPlanCompilation.SCHEMA) {
      readFromResource(s"explain/stream/analyzer/${testName}_SCHEMA.out")
    } else {
      readFromResource(s"explain/stream/analyzer/${testName}_IDENTIFIER.out")
    }
    assertEquals(getPrettyJson(expected), getPrettyJson(actual))
  }
}

object PlanAnalyzerTest {
  @Parameterized.Parameters(name = "compilation={0}")
  def parameters(): util.Collection[CatalogPlanCompilation] = {
    util.Arrays.asList(
      CatalogPlanCompilation.ALL,
      CatalogPlanCompilation.SCHEMA,
      CatalogPlanCompilation.IDENTIFIER)
  }
}
