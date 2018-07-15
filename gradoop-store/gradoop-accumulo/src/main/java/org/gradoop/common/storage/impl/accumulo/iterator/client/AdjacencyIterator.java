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

package org.gradoop.common.storage.impl.accumulo.iterator.client;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloAdjacencyHandler;
import org.gradoop.common.storage.iterator.ClosableIterator;
import org.gradoop.common.model.api.entites.EPGMAdjacencyRow;
import org.gradoop.common.model.impl.AdjacencyRow;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Vertex adjacency iterator
 */
public class AdjacencyIterator implements ClosableIterator<AdjacencyRow> {

  /**
   * Accumulo adjacency handler, for data convert
   */
  private final AccumuloAdjacencyHandler handler;

  /**
   * Accumulo batch scanner
   */
  private final BatchScanner scanner;

  /**
   * Adjacent query strategy
   */
  private final EPGMAdjacencyRow.Strategy strategy;

  /**
   * Inner data iterator
   */
  private final Iterator<Map.Entry<Key, Value>> iterator;

  /**
   * Client cache size
   */
  private final int cacheSize;

  /**
   * element cache size
   */
  private List<AdjacencyRow> cache = new ArrayList<>();

  /**
   * Create a new adjacent iterator instance
   *
   * @param scanner accumulo batch scanner
   * @param handler data handler
   * @param strategy query strategy
   * @param cacheSize client result cache size
   */
  public AdjacencyIterator(
    @Nonnull BatchScanner scanner,
    @Nonnull AccumuloAdjacencyHandler handler,
    @Nonnull EPGMAdjacencyRow.Strategy strategy,
    int cacheSize
  ) {
    this.scanner = scanner;
    this.handler = handler;
    this.strategy = strategy;
    this.cacheSize = cacheSize;
    this.iterator = scanner.iterator();
  }

  @Override
  public void close() {
    this.scanner.close();
  }

  @Override
  public boolean hasNext() {
    if (!cache.isEmpty()) {
      //cache is not empty
      return true;

    } else if (iterator.hasNext()) {
      //cache is empty, read elements to cache
      while (iterator.hasNext() && cache.size() < cacheSize) {
        AdjacencyRow next = null;
        switch (strategy) {
        case FROM_VERTEX_TO_EDGE:
          next = handler.readAdjacentFromVertex(iterator.next());
          break;
        case FROM_EDGE_TO_VERTEX:
          next = handler.readAdjacentFromEdge(iterator.next());
          break;
        default:
          break;
        }
        cache.add(next);
      }
      return hasNext();

    } else {
      //cache is empty and iterator has no element any more
      return false;

    }
  }

  @Override
  public AdjacencyRow next() {
    return cache.remove(0);
  }

}
