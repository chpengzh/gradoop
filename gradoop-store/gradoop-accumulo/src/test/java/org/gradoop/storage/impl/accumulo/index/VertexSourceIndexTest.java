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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.common.model.VertexSourceRow;
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
public class VertexSourceIndexTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "vertex_source_index_test_01";
  private static final String TEST02 = "vertex_source_index_test_02";
  private static final String TEST03 = "vertex_source_index_test_03";

  /**
   * Do insert by store and query all vertex source row from indexed data source
   *
   * @throws Throwable if test error
   */
  @Test
  public void test01_doInsertAndGetAll() throws Throwable {
    storeInsertAndTest(TEST01, (loader, store, config) -> {

      AccumuloIndexedDataSource source = new AccumuloIndexedDataSource(config, store);

      // Find all outcome and income edges, and combine as {vertex-id,edge-id}
      List<VertexSourceRow> input = Stream.concat(
        loader.getEdges().stream().map(it -> new VertexSourceRow(
          it.getSourceId(), it.getId(), VertexSourceRow.Strategy.OUTCOME)),
        loader.getEdges().stream().map(it -> new VertexSourceRow(
          it.getTargetId(), it.getId(), VertexSourceRow.Strategy.INCOME))
      ).collect(Collectors.toList());
      List<VertexSourceRow> query = source
        .getEdgeIdsFromVertexIds(VertexSourceRow.Strategy.BOTH)
        .collect();
      StoreTestUtils.validateVertexSourceRow(input, query);

      // Find income edges, and combine as {vertex-id,edge-id}
      input = loader.getEdges().stream()
        .map(it -> new VertexSourceRow(
          it.getTargetId(), it.getId(), VertexSourceRow.Strategy.INCOME))
        .collect(Collectors.toList());
      query = source
        .getEdgeIdsFromVertexIds(VertexSourceRow.Strategy.INCOME)
        .collect();
      StoreTestUtils.validateVertexSourceRow(input, query);

      // Find outcome edges, and combine as {vertex-id,edge-id}
      input = loader.getEdges().stream()
        .map(it -> new VertexSourceRow(
          it.getSourceId(), it.getId(), VertexSourceRow.Strategy.OUTCOME))
        .collect(Collectors.toList());
      query = source
        .getEdgeIdsFromVertexIds(VertexSourceRow.Strategy.OUTCOME)
        .collect();
      StoreTestUtils.validateVertexSourceRow(input, query);
    });
  }

  /**
   * Do import by data sink and query vertex source row from store
   *
   * @throws Throwable if test error
   */
  @Test
  public void test02_doImportAndQuery() throws Throwable {
    sinkImportAndTest(TEST02, (loader, store) -> {
      // All edge data
      List<Edge> edges = new ArrayList<>(loader.getEdges());
      List<Vertex> vertices = new ArrayList<>(loader.getVertices());

      for (int i = 0; i < 10; i++) {
        List<Vertex> samples = sample(new ArrayList<>(vertices), 5);
        GradoopIdSet seeds = GradoopIdSet.fromExisting(samples
          .stream()
          .map(Element::getId)
          .collect(Collectors.toList()));

        // Find all outcome and income edges, and combine as {vertex-id,edge-id}
        List<VertexSourceRow> input = Stream.concat(
          samples.stream().flatMap(vertex -> edges.stream()
            .filter(it -> it.getSourceId().equals(vertex.getId()))
            .map(it -> new VertexSourceRow(
              vertex.getId(), it.getId(), VertexSourceRow.Strategy.OUTCOME))),
          samples.stream().flatMap(vertex -> edges.stream()
            .filter(it -> it.getTargetId().equals(vertex.getId()))
            .map(it -> new VertexSourceRow(
              vertex.getId(), it.getId(), VertexSourceRow.Strategy.INCOME)))
        ).collect(Collectors.toList());
        List<VertexSourceRow> query = store
          .getEdgeIdsFromVertexIds(seeds, VertexSourceRow.Strategy.BOTH)
          .readRemainsAndClose();
        StoreTestUtils.validateVertexSourceRow(input, query);

        // Find income edges, and combine as {vertex-id,edge-id}
        input = samples.stream()
          .flatMap(vertex -> edges.stream()
            .filter(it -> it.getTargetId().equals(vertex.getId()))
            .map(it -> new VertexSourceRow(
              vertex.getId(), it.getId(), VertexSourceRow.Strategy.INCOME)))
          .collect(Collectors.toList());
        query = store.getEdgeIdsFromVertexIds(seeds, VertexSourceRow.Strategy.INCOME)
          .readRemainsAndClose();
        StoreTestUtils.validateVertexSourceRow(input, query);

        // Find outcome edges, and combine as {vertex-id,edge-id}
        input = samples.stream()
          .flatMap(vertex -> edges.stream()
            .filter(it -> it.getSourceId().equals(vertex.getId()))
            .map(it -> new VertexSourceRow(
              vertex.getId(), it.getId(), VertexSourceRow.Strategy.OUTCOME)))
          .collect(Collectors.toList());
        query = store.getEdgeIdsFromVertexIds(seeds, VertexSourceRow.Strategy.OUTCOME)
          .readRemainsAndClose();
        StoreTestUtils.validateVertexSourceRow(input, query);
      }
    });
  }

  /**
   * Do insert by store, pick some vertex seeds,
   * query isolated vertex source row from indexed data source
   *
   * @throws Throwable if test error
   */
  @Test
  public void test03_doInsertAndQuery() throws Throwable {
    storeInsertAndTest(TEST03, (loader, store, config) -> {
      // All edge data
      List<Edge> edges = new ArrayList<>(loader.getEdges());
      List<Vertex> vertices = new ArrayList<>(loader.getVertices());

      for (int i = 0; i < 10; i++) {
        List<Vertex> samples = sample(vertices, 5);
        List<GradoopId> seeds = samples
          .stream()
          .map(Element::getId)
          .collect(Collectors.toList());

        AccumuloIndexedDataSource source = new AccumuloIndexedDataSource(config, store);

        // Find all outcome and income edges, and combine as {vertex-id,edge-id}
        List<VertexSourceRow> input = Stream.concat(
          samples.stream().flatMap(vertex -> edges.stream()
            .filter(it -> it.getSourceId().equals(vertex.getId()))
            .map(it -> new VertexSourceRow(
              vertex.getId(), it.getId(), VertexSourceRow.Strategy.OUTCOME))),
          samples.stream().flatMap(vertex -> edges.stream()
            .filter(it -> it.getTargetId().equals(vertex.getId()))
            .map(it -> new VertexSourceRow(
              vertex.getId(), it.getId(), VertexSourceRow.Strategy.INCOME)))
        ).collect(Collectors.toList());
        List<VertexSourceRow> query = source
          .getEdgeIdsFromVertexIds(getExecutionEnvironment().fromCollection(seeds),
            VertexSourceRow.Strategy.BOTH)
          .collect();
        StoreTestUtils.validateVertexSourceRow(input, query);

        // Find income edges, and combine as {vertex-id,edge-id}
        input = samples.stream()
          .flatMap(vertex -> edges.stream()
            .filter(it -> it.getTargetId().equals(vertex.getId()))
            .map(it -> new VertexSourceRow(
              vertex.getId(), it.getId(), VertexSourceRow.Strategy.INCOME)))
          .collect(Collectors.toList());
        query = source
          .getEdgeIdsFromVertexIds(getExecutionEnvironment().fromCollection(seeds),
            VertexSourceRow.Strategy.INCOME)
          .collect();
        StoreTestUtils.validateVertexSourceRow(input, query);

        // Find outcome edges, and combine as {vertex-id,edge-id}
        input = samples.stream()
          .flatMap(vertex -> edges.stream()
            .filter(it -> it.getSourceId().equals(vertex.getId()))
            .map(it -> new VertexSourceRow(
              vertex.getId(), it.getId(), VertexSourceRow.Strategy.OUTCOME)))
          .collect(Collectors.toList());
        query = source
          .getEdgeIdsFromVertexIds(getExecutionEnvironment().fromCollection(seeds),
            VertexSourceRow.Strategy.OUTCOME)
          .collect();
        StoreTestUtils.validateVertexSourceRow(input, query);
      }
    });
  }

}
