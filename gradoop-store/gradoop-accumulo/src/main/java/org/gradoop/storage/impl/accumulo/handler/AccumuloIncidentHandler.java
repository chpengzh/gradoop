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

package org.gradoop.storage.impl.accumulo.handler;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.storage.common.model.EdgeSourceRow;
import org.gradoop.storage.common.model.VertexSourceRow;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Accumulo incident record handler
 */
public class AccumuloIncidentHandler implements Serializable {

  /**
   * Write edge income record to incident table
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
   * Write edge outcome record to incident table
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
   * Decode an accumulo client row to vertex source row
   *
   * @param row accumulo client row
   * @return vertex source row instance
   */
  @Nonnull
  public VertexSourceRow readFromVertex(@Nonnull Map.Entry<Key, Value> row) {
    VertexSourceRow result = new VertexSourceRow();
    result.setSourceVertexId(GradoopId.fromString(row.getKey().getRow().toString()));
    result.setIsolatedEdgeId(GradoopId.fromString(row.getKey().getColumnQualifier().toString()));
    result.setStrategy(
      Objects.equals(row.getKey().getColumnFamily().toString(), AccumuloTables.KEY.EDGE_IN) ?
        VertexSourceRow.Strategy.INCOME : VertexSourceRow.Strategy.OUTCOME);
    return result;
  }

  /**
   * Decode an accumulo client row to edge source row
   *
   * @param row accumulo client row
   * @return edge source row instance
   */
  @Nonnull
  public EdgeSourceRow readFromEdge(@Nonnull Map.Entry<Key, Value> row) {
    EdgeSourceRow result = new EdgeSourceRow();
    result.setSourceEdgeId(GradoopId.fromString(row.getKey().getRow().toString()));
    result.setIsolatedVertexId(GradoopId.fromString(row.getValue().toString()));
    result.setStrategy(
      Objects.equals(row.getKey().getColumnFamily().toString(), AccumuloTables.KEY.SOURCE) ?
        EdgeSourceRow.Strategy.SOURCE : EdgeSourceRow.Strategy.TARGET);
    return result;
  }

}


