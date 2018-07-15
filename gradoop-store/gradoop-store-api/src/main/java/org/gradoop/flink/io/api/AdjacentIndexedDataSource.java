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

package org.gradoop.flink.io.api;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.AdjacencyRow;
import org.gradoop.common.model.impl.id.GradoopId;

import javax.annotation.Nonnull;

/**
 * Data source with support for adjacent index. A Source
 * extending this interface is able to seek edge adjacency,
 * which means you can query edge ids by given a sets of vertex ids,
 * or query vertex ids by given a sets of edge ids
 */
public interface AdjacentIndexedDataSource {

  /**
   * Returns a set if edge ids that relative to vertex seeds
   *
   * @param vertexSeeds vertex seed id set
   * @param fetchEdgeIn should result contains vertex's income edge
   * @param fetchEdgeOut should result contains vertex's outcome edge
   * @return adjacency row as {vertex_id,edge_id}
   */
  @Nonnull
  DataSet<AdjacencyRow> adjacentFromVertices(
    @Nonnull DataSet<GradoopId> vertexSeeds,
    boolean fetchEdgeIn,
    boolean fetchEdgeOut
  );

  /**
   * Returns a set of vertex ids that relative to edge seeds
   *
   * @param edgeSeeds edge seed id set
   * @param fetchEdgeSource should result contains edge's source vertex
   * @param fetchEdgeTarget should result contains edge's target vertex
   * @return adjacency row as {edge_id,vertex_id}
   */
  @Nonnull
  DataSet<AdjacencyRow> adjacentFromEdges(
    @Nonnull DataSet<GradoopId> edgeSeeds,
    boolean fetchEdgeSource,
    boolean fetchEdgeTarget
  );

}
