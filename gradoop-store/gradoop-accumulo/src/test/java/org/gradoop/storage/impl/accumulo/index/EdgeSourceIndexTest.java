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

package org.gradoop.storage.impl.accumulo.index;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.storage.common.model.EdgeSourceRow;
import org.gradoop.storage.common.utils.StoreTestUtils;
import org.gradoop.storage.impl.accumulo.AccumuloStoreTestBase;
import org.gradoop.storage.impl.accumulo.io.AccumuloIndexedDataSource;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EdgeSourceIndexTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "edge_source_index_test_01";
  private static final String TEST02 = "edge_source_index_test_02";
  private static final String TEST03 = "edge_source_index_test_03";

  /**
   * Do insert by store and query all edge source row by indexed data source
   *
   * @throws Throwable if test error
   */
  @Test
  public void test01_doInsertAndGetAll() throws Throwable {
    storeInsertAndTest(TEST01, (loader, store, config) -> {

      AccumuloIndexedDataSource source = new AccumuloIndexedDataSource(config, store);

      // Find all source vertex and target vertex id set, and combine as {edge-id,vertex-id}
      List<EdgeSourceRow> input = Stream.concat(
        loader.getEdges().stream().map(it -> new EdgeSourceRow(
          it.getId(),
          it.getSourceId(),
          EdgeSourceRow.Strategy.SOURCE)),
        loader.getEdges().stream().map(it -> new EdgeSourceRow(
          it.getId(),
          it.getTargetId(),
          EdgeSourceRow.Strategy.TARGET))
      ).collect(Collectors.toList());
      List<EdgeSourceRow> query = source
        .getVertexIdsFromEdgeIds(EdgeSourceRow.Strategy.BOTH)
        .collect();
      StoreTestUtils.validateEdgeSourceRow(input, query);

      // Find source vertex id set, and combine as {edge-id,vertex-id}
      input = loader.getEdges().stream()
        .map(it -> new EdgeSourceRow(it.getId(), it.getSourceId(), EdgeSourceRow.Strategy.SOURCE))
        .collect(Collectors.toList());
      query = source
        .getVertexIdsFromEdgeIds(EdgeSourceRow.Strategy.SOURCE)
        .collect();
      StoreTestUtils.validateEdgeSourceRow(input, query);

      // Find target vertex id set, and combine as {edge-id,vertex-id}
      input = loader.getEdges().stream()
        .map(it -> new EdgeSourceRow(it.getId(), it.getTargetId(), EdgeSourceRow.Strategy.TARGET))
        .collect(Collectors.toList());
      query = source
        .getVertexIdsFromEdgeIds(EdgeSourceRow.Strategy.TARGET)
        .collect();
      StoreTestUtils.validateEdgeSourceRow(input, query);
    });
  }

  /**
   * Do import by data sink and query edge source row from store
   *
   * @throws Throwable if test error
   */
  @Test
  public void test02_doImportAndQuery() throws Throwable {
    sinkImportAndTest(TEST02, (loader, store) -> {
      for (int i = 0; i < 10; i++) {
        List<Edge> samples = sample(new ArrayList<>(loader.getEdges()), 5);
        GradoopIdSet seeds = GradoopIdSet.fromExisting(samples
          .stream()
          .map(Element::getId)
          .collect(Collectors.toSet()));

        // Find all source vertex and target vertex id set, and combine as {edge-id,vertex-id}
        List<EdgeSourceRow> input = Stream.concat(
          samples.stream().map(it -> new EdgeSourceRow(
            it.getId(),
            it.getSourceId(),
            EdgeSourceRow.Strategy.SOURCE)),
          samples.stream().map(it -> new EdgeSourceRow(
            it.getId(),
            it.getTargetId(),
            EdgeSourceRow.Strategy.TARGET))
        ).collect(Collectors.toList());
        List<EdgeSourceRow> query = store
          .getVertexIdsFromEdgeIds(seeds, EdgeSourceRow.Strategy.BOTH)
          .readRemainsAndClose();
        StoreTestUtils.validateEdgeSourceRow(input, query);

        // Find source vertex id set, and combine as {edge-id,vertex-id}
        input = samples.stream()
          .map(it -> new EdgeSourceRow(it.getId(), it.getSourceId(), EdgeSourceRow.Strategy.SOURCE))
          .collect(Collectors.toList());
        query = store
          .getVertexIdsFromEdgeIds(seeds, EdgeSourceRow.Strategy.SOURCE)
          .readRemainsAndClose();
        StoreTestUtils.validateEdgeSourceRow(input, query);

        // Find target vertex id set, and combine as {edge-id,vertex-id}
        input = samples.stream()
          .map(it -> new EdgeSourceRow(it.getId(), it.getTargetId(), EdgeSourceRow.Strategy.TARGET))
          .collect(Collectors.toList());
        query = store
          .getVertexIdsFromEdgeIds(seeds, EdgeSourceRow.Strategy.TARGET)
          .readRemainsAndClose();
        StoreTestUtils.validateEdgeSourceRow(input, query);
      }
    });
  }

  /**
   * Do insert by store, pick some edge seeds,
   * and query isolated edge source row from indexed data source
   *
   * @throws Throwable if test error
   */
  @Test
  public void test03_doInsertAndQuery() throws Throwable {
    storeInsertAndTest(TEST03, (loader, store, config) -> {
      // All edge data
      List<Edge> edges = new ArrayList<>(loader.getEdges());

      for (int i = 0; i < 10; i++) {
        List<Edge> samples = sample(edges, 5);
        List<GradoopId> seeds = samples
          .stream()
          .map(Element::getId)
          .collect(Collectors.toList());

        AccumuloIndexedDataSource source = new AccumuloIndexedDataSource(config, store);

        // Find all source vertex and target vertex id set, and combine as {edge-id,vertex-id}
        List<EdgeSourceRow> input = Stream.concat(
          samples.stream().map(it -> new EdgeSourceRow(
            it.getId(),
            it.getSourceId(),
            EdgeSourceRow.Strategy.SOURCE)),
          samples.stream().map(it -> new EdgeSourceRow(
            it.getId(),
            it.getTargetId(),
            EdgeSourceRow.Strategy.TARGET))
        ).collect(Collectors.toList());
        List<EdgeSourceRow> query = source
          .getVertexIdsFromEdgeIds(getExecutionEnvironment().fromCollection(seeds),
            EdgeSourceRow.Strategy.BOTH)
          .collect();
        StoreTestUtils.validateEdgeSourceRow(input, query);

        // Find source vertex id set, and combine as {edge-id,vertex-id}
        input = samples.stream()
          .map(it -> new EdgeSourceRow(it.getId(), it.getSourceId(), EdgeSourceRow.Strategy.SOURCE))
          .collect(Collectors.toList());
        query = source
          .getVertexIdsFromEdgeIds(getExecutionEnvironment().fromCollection(seeds),
            EdgeSourceRow.Strategy.SOURCE)
          .collect();
        StoreTestUtils.validateEdgeSourceRow(input, query);

        // Find target vertex id set, and combine as {edge-id,vertex-id}
        input = samples.stream()
          .map(it -> new EdgeSourceRow(it.getId(), it.getTargetId(), EdgeSourceRow.Strategy.TARGET))
          .collect(Collectors.toList());
        query = source
          .getVertexIdsFromEdgeIds(getExecutionEnvironment().fromCollection(seeds),
            EdgeSourceRow.Strategy.TARGET)
          .collect();
        StoreTestUtils.validateEdgeSourceRow(input, query);
      }
    });
  }

}
