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
public class VertexSourceRow extends Tuple3<GradoopId, GradoopId, VertexSourceRow.Strategy>
  implements Comparable<VertexSourceRow> {

  /**
   * None args constructor for serializable
   */
  public VertexSourceRow() {
    this(GradoopId.NULL_VALUE, GradoopId.NULL_VALUE, Strategy.INCOME);
  }

  /**
   * Create a new vertex source row
   *
   * @param seedVertexId query seed id
   * @param isolatedEdgeId result adjacent id
   * @param strategy query strategy
   */
  public VertexSourceRow(
    @Nonnull GradoopId seedVertexId,
    @Nonnull GradoopId isolatedEdgeId,
    @Nonnull Strategy strategy
  ) {
    this.f0 = seedVertexId;
    this.f1 = isolatedEdgeId;
    this.f2 = strategy;
  }

  @Nonnull
  public GradoopId getSourceVertexId() {
    return f0;
  }

  @Nonnull
  public GradoopId getIsolatedEdgeId() {
    return f1;
  }

  @Nonnull
  public Strategy getStrategy() {
    return f2;
  }

  public void setSourceVertexId(@Nonnull GradoopId seedId) {
    this.f0 = seedId;
  }

  public void setIsolatedEdgeId(@Nonnull GradoopId adjacentId) {
    this.f1 = adjacentId;
  }

  public void setStrategy(@Nonnull Strategy strategy) {
    this.f2 = strategy;
  }

  @Override
  public int compareTo(@Nonnull VertexSourceRow o) {
    return Integer.compare(f0.compareTo(o.f0), 0) * (0b1 << 2) +
      Integer.compare(f1.compareTo(o.f1), 0) * (0b1 << 1) +
      Integer.compare(f2.compareTo(o.f2), 0);
  }

  /**
   * Isolation strategy
   */
  public enum Strategy {

    /**
     * As for vertex(v) and edge(e), v is incident from e
     *
     *    (v) <-[e]- ()
     */
    INCOME,

    /**
     * As for vertex(v) and edge(e), v is incident to e
     *
     *    (v) -[e]-> ()
     */
    OUTCOME,

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
   * Vertex source row => source vertex id
   */
  public static class VertexId implements
    MapFunction<VertexSourceRow, GradoopId>,
    KeySelector<VertexSourceRow, GradoopId> {

    @Override
    public GradoopId map(VertexSourceRow element) {
      return element.getSourceVertexId();
    }

    @Override
    public GradoopId getKey(VertexSourceRow element) {
      return element.getSourceVertexId();
    }

  }


  /**
   * Vertex source row => isolated edge id
   */
  public static class EdgeId implements
    MapFunction<VertexSourceRow, GradoopId>,
    KeySelector<VertexSourceRow, GradoopId> {

    @Override
    public GradoopId map(VertexSourceRow element) {
      return element.getIsolatedEdgeId();
    }

    @Override
    public GradoopId getKey(VertexSourceRow element) {
      return element.getIsolatedEdgeId();
    }
  }


  /**
   * Vertex source row => isolation type (income/outcome)
   */
  public static class StrategyType implements
    MapFunction<VertexSourceRow, Strategy>,
    KeySelector<VertexSourceRow, Strategy> {

    @Override
    public VertexSourceRow.Strategy map(VertexSourceRow epgmAdjacentRow) {
      return epgmAdjacentRow.getStrategy();
    }

    @Override
    public VertexSourceRow.Strategy getKey(VertexSourceRow epgmAdjacentRow) {
      return epgmAdjacentRow.getStrategy();
    }
  }

}
