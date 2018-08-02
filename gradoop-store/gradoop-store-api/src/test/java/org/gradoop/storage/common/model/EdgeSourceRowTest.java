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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class EdgeSourceRowTest extends GradoopFlinkTestBase {

  private final Random random = new Random(System.currentTimeMillis());

  @Test
  public void equalityTest() {
    for (int i = 0; i < 10; i++) {
      EdgeSourceRow sample = createNew();
      EdgeSourceRow test = new EdgeSourceRow(
        GradoopId.fromString(sample.getSourceEdgeId().toString()),
        GradoopId.fromString(sample.getIsolatedVertexId().toString()),
        sample.getStrategy());
      Assert.assertEquals(sample, test);
    }
  }

  @Test
  public void compareTest() {
    ArrayList<EdgeSourceRow> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(createNew());
    }
    List<EdgeSourceRow> first = new ArrayList<>(rows);
    List<EdgeSourceRow> second = new ArrayList<>(rows);
    first.sort(EdgeSourceRow::compareTo);
    second.sort(EdgeSourceRow::compareTo);

    Assert.assertEquals(first.size(), second.size());
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(first.get(i), second.get(i));
    }
  }

  @Test
  public void seedIdMapper() throws Exception {
    EdgeSourceRow origin = createNew();
    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<EdgeSourceRow> adjacencyDataSet = env.fromCollection(Lists.newArrayList(origin));

    List<GradoopId> query = adjacencyDataSet
      .map(new EdgeSourceRow.EdgeId())
      .collect();
    Assert.assertEquals(query.size(), 1);
    Assert.assertEquals(query.get(0), origin.getSourceEdgeId());
  }

  @Test
  public void isolatedIdMapper() throws Exception {
    EdgeSourceRow origin = createNew();
    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<EdgeSourceRow> adjacencyDataSet = env.fromCollection(Lists.newArrayList(origin));

    List<GradoopId> query = adjacencyDataSet
      .map(new EdgeSourceRow.VertexId())
      .collect();
    Assert.assertEquals(query.size(), 1);
    Assert.assertEquals(query.get(0), origin.getIsolatedVertexId());
  }

  @Test
  public void strategyMapper() throws Exception {
    EdgeSourceRow origin = createNew();
    ExecutionEnvironment env = getExecutionEnvironment();
    DataSet<EdgeSourceRow> adjacencyDataSet = env.fromCollection(Lists.newArrayList(origin));

    List<EdgeSourceRow.Strategy> query = adjacencyDataSet
      .map(new EdgeSourceRow.StrategyType())
      .collect();
    Assert.assertEquals(query.size(), 1);
    Assert.assertEquals(query.get(0), origin.getStrategy());
  }

  private EdgeSourceRow createNew() {
    String seedId = GradoopId.get().toString();
    String isolated = GradoopId.get().toString();
    int typeOrdinary = random.nextInt(EdgeSourceRow.Strategy.values().length);
    EdgeSourceRow.Strategy strategy = EdgeSourceRow.Strategy.values()[typeOrdinary];

    return new EdgeSourceRow(
      GradoopId.fromString(seedId),
      GradoopId.fromString(isolated),
      strategy);
  }

}
