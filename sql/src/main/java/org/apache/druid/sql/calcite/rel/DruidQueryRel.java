/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql.calcite.rel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.sql.calcite.external.ExternalTableScan;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.InlineTable;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * DruidRel that operates on top of a {@link DruidTable} directly (no joining or subqueries).
 */
public class DruidQueryRel extends DruidRel<DruidQueryRel>
{
  @Nullable
  private final RelOptTable table; // must not be null except for inline data
  private final DruidTable druidTable;
  private final PartialDruidQuery partialQuery;

  private DruidQueryRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      @Nullable final RelOptTable table,
      final DruidTable druidTable,
      final PlannerContext plannerContext,
      final PartialDruidQuery partialQuery
  )
  {
    super(cluster, traitSet, plannerContext);
    this.table = table;
    this.druidTable = Preconditions.checkNotNull(druidTable, "druidTable");
    this.partialQuery = Preconditions.checkNotNull(partialQuery, "partialQuery");
  }

  /**
   * Create a DruidQueryRel representing a full scan of a builtin table or lookup.
   */
  public static DruidQueryRel scanTable(
      final LogicalTableScan scanRel,
      final RelOptTable table,
      final DruidTable druidTable,
      final PlannerContext plannerContext
  )
  {
    return new DruidQueryRel(
        scanRel.getCluster(),
        scanRel.getCluster().traitSetOf(Convention.NONE),
        Preconditions.checkNotNull(table, "table"),
        druidTable,
        plannerContext,
        PartialDruidQuery.create(scanRel)
    );
  }

  /**
   * Create a DruidQueryRel representing a full scan of external data.
   */
  public static DruidQueryRel scanExternal(
      final ExternalTableScan scanRel,
      final PlannerContext plannerContext
  )
  {
    return new DruidQueryRel(
        scanRel.getCluster(),
        scanRel.getCluster().traitSetOf(Convention.NONE),
        null,
        scanRel.getDruidTable(),
        plannerContext,
        PartialDruidQuery.create(scanRel)
    );
  }

  /**
   * Create a DruidQueryRel representing a full scan of inline, literal values.
   */
  public static DruidQueryRel scanConstantRel(
      final RelNode rel,
      final InlineDataSource dataSource,
      final PlannerContext plannerContext
  )
  {
    return new DruidQueryRel(
        rel.getCluster(),
        rel.getTraitSet().replace(Convention.NONE), // keep traitSet of input rel, except for convention
        null,
        new InlineTable(dataSource),
        plannerContext,
        PartialDruidQuery.create(rel)
    );
  }

  @Override
  public DruidQuery toDruidQuery(final boolean finalizeAggregations)
  {
    return partialQuery.build(
        druidTable.getDataSource(),
        druidTable.getRowSignature(),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations,
        true
    );
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return partialQuery.build(
        druidTable.getDataSource(),
        druidTable.getRowSignature(),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        false,
        false
    );
  }

  @Override
  public DruidQueryRel asDruidConvention()
  {
    return new DruidQueryRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        table,
        druidTable,
        getPlannerContext(),
        partialQuery
    );
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    return druidTable.getDataSource().getTableNames();
  }

  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return partialQuery;
  }

  @Override
  public DruidQueryRel withPartialQuery(final PartialDruidQuery newQueryBuilder)
  {
    return new DruidQueryRel(
        getCluster(),
        newQueryBuilder.getTraitSet(getConvention(), getPlannerContext()),
        table,
        druidTable,
        getPlannerContext(),
        newQueryBuilder
    );
  }

  public DruidTable getDruidTable()
  {
    return druidTable;
  }

  @Override
  public RelOptTable getTable()
  {
    return table;
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }

  @Override
  public RelWriter explainTerms(final RelWriter pw)
  {
    final String queryString;
    final DruidQuery druidQuery = toDruidQueryForExplaining();

    try {
      queryString = getPlannerContext().getJsonMapper().writeValueAsString(druidQuery.getQuery());
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return pw.item("query", queryString)
             .item("signature", druidQuery.getOutputRowSignature());
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    return planner.getCostFactory().makeCost(partialQuery.estimateCost(), 0, 0);
  }
}
