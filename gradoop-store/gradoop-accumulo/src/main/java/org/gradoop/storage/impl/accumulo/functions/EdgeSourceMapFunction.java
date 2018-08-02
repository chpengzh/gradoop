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

package org.gradoop.storage.impl.accumulo.functions;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.storage.common.api.EPGMGraphOutput;
import org.gradoop.storage.common.model.EdgeSourceRow;
import org.gradoop.storage.config.GradoopAccumuloConfig;
import org.gradoop.storage.impl.accumulo.AccumuloEPGMStore;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Map edge seeds to its edge source row
 *
 * EdgeID => EdgeSourceRow
 */
public class EdgeSourceMapFunction implements MapPartitionFunction<GradoopId, EdgeSourceRow> {

  /**
   * Should result contains edge income
   */
  private final GradoopAccumuloConfig config;

  /**
   * Should result contains edge outcome
   */
  private final EdgeSourceRow.Strategy strategy;

  /**
   * Create a new edge source mapping function
   *
   * @param config gradoop accumulo configuration
   * @param strategy fetch strategy
   */
  public EdgeSourceMapFunction(
    @Nonnull GradoopAccumuloConfig config,
    EdgeSourceRow.Strategy strategy
  ) {
    this.config = config;
    this.strategy = strategy;
  }

  @Override
  public void mapPartition(
    Iterable<GradoopId> ids,
    Collector<EdgeSourceRow> out
  ) throws Exception {
    //create a epgm store in each partition
    AccumuloEPGMStore store = new AccumuloEPGMStore(config);
    Iterator<GradoopId> iterator = ids.iterator();
    do {
      List<GradoopId> split = new ArrayList<>();
      for (int i = 0; i < EPGMGraphOutput.DEFAULT_CACHE_SIZE && iterator.hasNext(); i++) {
        split.add(iterator.next());
      }
      store.getVertexIdsFromEdgeIds(GradoopIdSet.fromExisting(split), strategy)
        .readRemainsAndClose()
        .forEach(out::collect);

    } while (iterator.hasNext());
  }

}
