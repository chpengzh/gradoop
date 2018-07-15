/*
 * Copyright © Since 2018 www.isinonet.com Company
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradoop.flink.model.impl.functions.adjacency;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.api.entites.EPGMAdjacencyRow;
import org.gradoop.common.model.impl.AdjacencyRow;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class AdjacentOperationTest extends GradoopFlinkTestBase {

  @Test
  public void seedIdMapper() throws Exception {
    AdjacencyRow origin = new AdjacencyRow(
      GradoopId.get(),
      GradoopId.get(),
      EPGMAdjacencyRow.Strategy.FROM_VERTEX_TO_EDGE);

    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<AdjacencyRow> adjacencyDataSet = env.fromCollection(Lists.newArrayList(origin));

    List<GradoopId> query = adjacencyDataSet
      .map(new SeedId<>())
      .collect();
    Assert.assertEquals(query.size(), 1);
    Assert.assertEquals(query.get(0), origin.getSeedId());
  }

  @Test
  public void adjacentIdMapper() throws Exception {
    AdjacencyRow origin = new AdjacencyRow(
      GradoopId.get(),
      GradoopId.get(),
      EPGMAdjacencyRow.Strategy.FROM_VERTEX_TO_EDGE);

    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<AdjacencyRow> adjacencyDataSet = env.fromCollection(Lists.newArrayList(origin));

    List<GradoopId> query = adjacencyDataSet
      .map(new AdjacentId<>())
      .collect();
    Assert.assertEquals(query.size(), 1);
    Assert.assertEquals(query.get(0), origin.getAdjacentId());
  }

  @Test
  public void strategyMapper() throws Exception {
    AdjacencyRow origin = new AdjacencyRow(
      GradoopId.get(),
      GradoopId.get(),
      EPGMAdjacencyRow.Strategy.FROM_VERTEX_TO_EDGE);

    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<AdjacencyRow> adjacencyDataSet = env.fromCollection(Lists.newArrayList(origin));

    List<EPGMAdjacencyRow.Strategy> query = adjacencyDataSet
      .map(new AdjacentStrategy<>())
      .collect();
    Assert.assertEquals(query.size(), 1);
    Assert.assertEquals(query.get(0), origin.getStrategy());
  }

}
