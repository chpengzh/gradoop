/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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

package org.gradoop.storage.common.model;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class VertexSourceRowTest extends GradoopFlinkTestBase {

  private final Random random = new Random(System.currentTimeMillis());

  @Test
  public void equalityTest() {
    for (int i = 0; i < 10; i++) {
      VertexSourceRow sample = createNew();
      VertexSourceRow test = new VertexSourceRow(
        GradoopId.fromString(sample.getSourceVertexId().toString()),
        GradoopId.fromString(sample.getIsolatedEdgeId().toString()),
        sample.getStrategy());
      Assert.assertEquals(sample, test);
    }
  }

  @Test
  public void compareTest() {
    ArrayList<VertexSourceRow> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(createNew());
    }
    List<VertexSourceRow> first = new ArrayList<>(rows);
    List<VertexSourceRow> second = new ArrayList<>(rows);
    first.sort(VertexSourceRow::compareTo);
    second.sort(VertexSourceRow::compareTo);

    Assert.assertEquals(first.size(), second.size());
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(first.get(i), second.get(i));
    }
  }

  @Test
  public void seedIdMapper() throws Exception {
    VertexSourceRow origin = createNew();
    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<VertexSourceRow> adjacencyDataSet = env.fromCollection(Lists.newArrayList(origin));

    List<GradoopId> query = adjacencyDataSet
      .map(new VertexSourceRow.VertexId())
      .collect();
    Assert.assertEquals(query.size(), 1);
    Assert.assertEquals(query.get(0), origin.getSourceVertexId());
  }

  @Test
  public void adjacentIdMapper() throws Exception {
    VertexSourceRow origin = createNew();
    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<VertexSourceRow> adjacencyDataSet = env.fromCollection(Lists.newArrayList(origin));

    List<GradoopId> query = adjacencyDataSet
      .map(new VertexSourceRow.EdgeId())
      .collect();
    Assert.assertEquals(query.size(), 1);
    Assert.assertEquals(query.get(0), origin.getIsolatedEdgeId());
  }

  @Test
  public void strategyMapper() throws Exception {
    VertexSourceRow origin = createNew();
    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<VertexSourceRow> adjacencyDataSet = env.fromCollection(Lists.newArrayList(origin));

    List<VertexSourceRow.Strategy> query = adjacencyDataSet
      .map(new VertexSourceRow.StrategyType())
      .collect();
    Assert.assertEquals(query.size(), 1);
    Assert.assertEquals(query.get(0), origin.getStrategy());
  }

  private VertexSourceRow createNew() {
    String seedId = GradoopId.get().toString();
    String adjacentId = GradoopId.get().toString();
    int typeOrdinary = random.nextInt(VertexSourceRow.Strategy.values().length);
    VertexSourceRow.Strategy strategy = VertexSourceRow.Strategy.values()[typeOrdinary];

    return new VertexSourceRow(
      GradoopId.fromString(seedId),
      GradoopId.fromString(adjacentId),
      strategy);
  }

}
