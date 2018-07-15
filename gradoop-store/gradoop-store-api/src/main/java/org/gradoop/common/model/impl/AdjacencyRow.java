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

package org.gradoop.common.model.impl;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.api.entites.EPGMAdjacencyRow;

import javax.annotation.Nonnull;
import java.util.Arrays;

/**
 * Default Adjacent row implement
 */
public class AdjacencyRow implements EPGMAdjacencyRow, Comparable<AdjacencyRow> {

  /**
   * Query seed element id
   */
  private GradoopId seedId;

  /**
   * Query result adjacent id
   */
  private GradoopId adjacentId;

  /**
   * Query strategy
   */
  private Strategy strategy;

  /**
   * None args constructor for serializable
   */
  public AdjacencyRow() {
    this(GradoopId.NULL_VALUE, GradoopId.NULL_VALUE, Strategy.FROM_EDGE_TO_VERTEX);
  }

  /**
   * Create a new adjacent row
   *
   * @param seedId query seed id
   * @param adjacentId result adjacent id
   * @param strategy query strategy
   */
  public AdjacencyRow(
    @Nonnull GradoopId seedId,
    @Nonnull GradoopId adjacentId,
    @Nonnull Strategy strategy
  ) {
    this.seedId = seedId;
    this.adjacentId = adjacentId;
    this.strategy = strategy;
  }

  @Nonnull
  @Override
  public GradoopId getSeedId() {
    return seedId;
  }

  @Nonnull
  @Override
  public GradoopId getAdjacentId() {
    return adjacentId;
  }

  @Nonnull
  @Override
  public Strategy getStrategy() {
    return strategy;
  }

  public void setSeedId(@Nonnull GradoopId seedId) {
    this.seedId = seedId;
  }

  public void setAdjacentId(@Nonnull GradoopId adjacentId) {
    this.adjacentId = adjacentId;
  }

  public void setStrategy(@Nonnull Strategy strategy) {
    this.strategy = strategy;
  }

  @Override
  public int compareTo(@Nonnull AdjacencyRow o) {
    return Integer.compare(seedId.compareTo(o.seedId), 0) * (0b1 << 2) +
      Integer.compare(adjacentId.compareTo(o.adjacentId), 0) * (0b1 << 1) +
      Integer.compare(strategy.compareTo(o.strategy), 0);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof AdjacencyRow &&
      Arrays.equals(toByteArray(), ((AdjacencyRow) obj).toByteArray());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(toByteArray());
  }

  /**
   * Convert a adjacent row to byte array
   *
   * @return byte array value
   */
  @Nonnull
  public byte[] toByteArray() {
    byte[] result = new byte[GradoopId.ID_SIZE * 2 + 1];
    System.arraycopy(seedId.toByteArray(), 0, result, 0, GradoopId.ID_SIZE);
    System.arraycopy(adjacentId.toByteArray(), 0, result, 0, GradoopId.ID_SIZE);
    result[GradoopId.ID_SIZE * 2] = (byte) strategy.ordinal();
    return result;
  }

  @Override
  public String toString() {
    return String.format("%s->%s:%s", seedId, adjacentId, strategy);
  }

}
