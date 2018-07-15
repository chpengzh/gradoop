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

package org.gradoop.common.storage.impl.accumulo.handler;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.common.model.api.entites.EPGMAdjacencyRow;
import org.gradoop.common.model.impl.AdjacencyRow;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Accumulo adjacency record handler
 */
public class AccumuloAdjacencyHandler {

  /**
   * Write edge income record to adjacency table
   *
   * @param mutation record mutation
   * @param edge edge content
   * @return mutation to be write
   */
  public Mutation writeIncomeEdge(
    @Nonnull Mutation mutation,
    @Nonnull EPGMEdge edge
  ) {
    mutation.put(/*cf*/ AccumuloTables.KEY.EDGE_IN,
      /*cq*/edge.getId().toString(),
      /*value*/edge.getSourceId().toString());
    return mutation;
  }

  /**
   * Write edge outcome record to adjacency table
   *
   * @param mutation record mutation
   * @param edge edge content
   * @return mutation to be write
   */
  public Mutation writeOutcomeEdge(
    @Nonnull Mutation mutation,
    @Nonnull EPGMEdge edge
  ) {
    mutation.put(/*cf*/ AccumuloTables.KEY.EDGE_OUT,
      /*cq*/edge.getId().toString(),
      /*value*/edge.getTargetId().toString());
    return mutation;
  }

  /**
   * Decode an accumulo client row to adjacent row
   *
   * @param row accumulo client row
   * @return adjacent row instance
   */
  @Nonnull
  public AdjacencyRow readAdjacentFromVertex(@Nonnull Map.Entry<Key, Value> row) {
    AdjacencyRow result = new AdjacencyRow();
    result.setSeedId(GradoopId.fromString(row.getKey().getRow().toString()));
    result.setAdjacentId(GradoopId.fromString(row.getKey().getColumnQualifier().toString()));
    result.setStrategy(EPGMAdjacencyRow.Strategy.FROM_VERTEX_TO_EDGE);
    return result;
  }

  /**
   * Decode an accumulo client row to adjacent row
   *
   * @param row accumulo client row
   * @return adjacent row instance
   */
  @Nonnull
  public AdjacencyRow readAdjacentFromEdge(@Nonnull Map.Entry<Key, Value> row) {
    AdjacencyRow result = new AdjacencyRow();
    result.setSeedId(GradoopId.fromString(row.getKey().getRow().toString()));
    result.setAdjacentId(GradoopId.fromString(row.getValue().toString()));
    result.setStrategy(EPGMAdjacencyRow.Strategy.FROM_EDGE_TO_VERTEX);
    return result;
  }

}


