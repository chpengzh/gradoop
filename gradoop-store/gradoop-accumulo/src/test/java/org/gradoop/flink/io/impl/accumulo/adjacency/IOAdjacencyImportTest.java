/**
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

package org.gradoop.flink.io.impl.accumulo.adjacency;

import org.gradoop.AccumuloStoreTestBase;
import org.gradoop.common.model.api.entites.EPGMAdjacencyRow;
import org.gradoop.common.model.impl.AdjacencyRow;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.utils.AdjacencyTestUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IOAdjacencyImportTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "io_adjacency_import01";
  private static final String TEST02 = "io_adjacency_import02";

  @Test
  public void test01_queryVerticesByEdgeIds() throws Throwable {
    final EPGMAdjacencyRow.Strategy strategy = EPGMAdjacencyRow.Strategy.FROM_EDGE_TO_VERTEX;
    sinkImportAndTest(TEST01, (loader, store) -> {
      for (int i = 0; i < 10; i++) {
        List<Edge> samples = sample(new ArrayList<>(loader.getEdges()), 5);
        GradoopIdSet seeds = GradoopIdSet.fromExisting(samples
          .stream()
          .map(Element::getId)
          .collect(Collectors.toSet()));

        // Find all source vertex and target vertex id set, and combine as {edge-id,vertex-id}
        List<AdjacencyRow> input = Stream.concat(
          samples.stream().map(it -> new AdjacencyRow(
            it.getId(),
            it.getSourceId(),
            strategy)),
          samples.stream().map(it -> new AdjacencyRow(
            it.getId(),
            it.getTargetId(),
            strategy))
        ).collect(Collectors.toList());
        List<AdjacencyRow> query = store.adjacentFromEdges(seeds,
          true,
          true)
          .readRemainsAndClose();
        AdjacencyTestUtils.validateAdjacency(input, query, strategy);

        // Find source vertex id set, and combine as {edge-id,vertex-id}
        input = samples.stream()
          .map(it -> new AdjacencyRow(it.getId(), it.getSourceId(), strategy))
          .collect(Collectors.toList());
        query = store.adjacentFromEdges(seeds,
          true,
          false)
          .readRemainsAndClose();
        AdjacencyTestUtils.validateAdjacency(input, query, strategy);

        // Find target vertex id set, and combine as {edge-id,vertex-id}
        input = samples.stream()
          .map(it -> new AdjacencyRow(it.getId(), it.getTargetId(), strategy))
          .collect(Collectors.toList());
        query = store.adjacentFromEdges(seeds,
          false,
          true)
          .readRemainsAndClose();
        AdjacencyTestUtils.validateAdjacency(input, query, strategy);
      }
    });
  }

  @Test
  public void test02_queryByVertexIds() throws Throwable {
    final EPGMAdjacencyRow.Strategy strategy = EPGMAdjacencyRow.Strategy.FROM_VERTEX_TO_EDGE;
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
        List<AdjacencyRow> input = Stream.concat(
          samples.stream().flatMap(vertex -> edges.stream()
            .filter(it -> it.getSourceId().equals(vertex.getId()))
            .map(it -> new AdjacencyRow(vertex.getId(), it.getId(), strategy))),
          samples.stream().flatMap(vertex -> edges.stream()
            .filter(it -> it.getTargetId().equals(vertex.getId()))
            .map(it -> new AdjacencyRow(vertex.getId(), it.getId(), strategy)))
        ).collect(Collectors.toList());
        List<AdjacencyRow> query = store.adjacentFromVertices(seeds,
          true,
          true)
          .readRemainsAndClose();
        AdjacencyTestUtils.validateAdjacency(input, query, strategy);

        // Find income edges, and combine as {vertex-id,edge-id}
        input = samples.stream()
          .flatMap(vertex -> edges.stream()
            .filter(it -> it.getTargetId().equals(vertex.getId()))
            .map(it -> new AdjacencyRow(vertex.getId(), it.getId(), strategy)))
          .collect(Collectors.toList());
        query = store.adjacentFromVertices(seeds,
          true,
          false)
          .readRemainsAndClose();
        AdjacencyTestUtils.validateAdjacency(input, query, strategy);

        // Find outcome edges, and combine as {vertex-id,edge-id}
        input = samples.stream()
          .flatMap(vertex -> edges.stream()
            .filter(it -> it.getSourceId().equals(vertex.getId()))
            .map(it -> new AdjacencyRow(vertex.getId(), it.getId(), strategy)))
          .collect(Collectors.toList());
        query = store.adjacentFromVertices(seeds,
          false,
          true)
          .readRemainsAndClose();
        AdjacencyTestUtils.validateAdjacency(input, query, strategy);
      }
    });
  }

}
