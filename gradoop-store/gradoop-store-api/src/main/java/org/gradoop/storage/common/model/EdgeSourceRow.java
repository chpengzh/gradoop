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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;

import javax.annotation.Nonnull;

/**
 * Vertex Source row
 */
public class EdgeSourceRow extends Tuple3<GradoopId, GradoopId, EdgeSourceRow.Strategy>
  implements Comparable<EdgeSourceRow> {

  /**
   * None args constructor for serializable
   */
  public EdgeSourceRow() {
  }

  /**
   * Create a new edge source row
   *
   * @param sourceEdgeId query seed id
   * @param isolatedVertexId result adjacent id
   * @param strategy query strategy
   */
  public EdgeSourceRow(
    @Nonnull GradoopId sourceEdgeId,
    @Nonnull GradoopId isolatedVertexId,
    @Nonnull Strategy strategy
  ) {
    super(sourceEdgeId, isolatedVertexId, strategy);
  }

  public GradoopId getSourceEdgeId() {
    return f0;
  }

  public void setSourceEdgeId(@Nonnull GradoopId sourceEdgeId) {
    this.f0 = sourceEdgeId;
  }

  public GradoopId getIsolatedVertexId() {
    return f1;
  }

  public void setIsolatedVertexId(@Nonnull GradoopId isolatedVertexId) {
    this.f1 = isolatedVertexId;
  }

  public Strategy getStrategy() {
    return f2;
  }

  public void setStrategy(@Nonnull Strategy strategy) {
    this.f2 = strategy;
  }

  @Override
  public int compareTo(@Nonnull EdgeSourceRow o) {
    return Integer.compare(f0.compareTo(o.f0), 0) * (0b1 << 2) +
      Integer.compare(f1.compareTo(o.f1), 0) * (0b1 << 1) +
      Integer.compare(f2.compareTo(o.f2), 0);
  }

  /**
   * Isolation strategy
   */
  public enum Strategy {

    /**
     * As for vertex(v) and edge(e), v is incident to e
     *
     *    (v) -[e]-> ()
     */
    SOURCE,

    /**
     * As for vertex(v) and edge(e), v is incident from e
     *
     *    (v) <-[e]- ()
     */
    TARGET,

    /**
     * Query both, this strategy is query internal,
     * which means the query result will not contains this flag.
     * As for vertex(v) and edge(e), v is isolated to e
     *
     *    (v) -[e]-> ()   OR   (v) <-[e]- ()
     */
    BOTH

  }

  /**
   * Edge source row => source edge id
   */
  public static class EdgeId implements
    KeySelector<EdgeSourceRow, GradoopId>,
    MapFunction<EdgeSourceRow, GradoopId> {

    @Override
    public GradoopId map(EdgeSourceRow edgeSourceRow) throws Exception {
      return edgeSourceRow.f0;
    }

    @Override
    public GradoopId getKey(EdgeSourceRow edgeSourceRow) throws Exception {
      return edgeSourceRow.f0;
    }
  }

  /**
   * Edge source row => isolated vertex id
   */
  public static class VertexId implements
    KeySelector<EdgeSourceRow, GradoopId>,
    MapFunction<EdgeSourceRow, GradoopId> {

    @Override
    public GradoopId map(EdgeSourceRow edgeSourceRow) throws Exception {
      return edgeSourceRow.f1;
    }

    @Override
    public GradoopId getKey(EdgeSourceRow edgeSourceRow) throws Exception {
      return edgeSourceRow.f1;
    }
  }

  /**
   * Edge source row => isolated strategy
   */
  public static class StrategyType implements
    KeySelector<EdgeSourceRow, Strategy>,
    MapFunction<EdgeSourceRow, Strategy> {

    @Override
    public Strategy map(EdgeSourceRow edgeSourceRow) throws Exception {
      return edgeSourceRow.f2;
    }

    @Override
    public Strategy getKey(EdgeSourceRow edgeSourceRow) throws Exception {
      return edgeSourceRow.f2;
    }
  }
}
