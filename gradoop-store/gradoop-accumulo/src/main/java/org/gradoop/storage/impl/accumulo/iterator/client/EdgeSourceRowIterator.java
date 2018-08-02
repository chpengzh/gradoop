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

package org.gradoop.storage.impl.accumulo.iterator.client;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.gradoop.storage.common.iterator.ClosableIterator;
import org.gradoop.storage.common.model.EdgeSourceRow;
import org.gradoop.storage.impl.accumulo.handler.AccumuloIncidentHandler;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Edge source row iterator
 */
public class EdgeSourceRowIterator implements ClosableIterator<EdgeSourceRow> {

  /**
   * Accumulo incident handler, for data convert
   */
  private final AccumuloIncidentHandler handler;

  /**
   * Accumulo batch scanner
   */
  private final BatchScanner scanner;

  /**
   * Inner data iterator
   */
  private final Iterator<Map.Entry<Key, Value>> iterator;

  /**
   * Client cache size
   */
  private final int cacheSize;

  /**
   * Element cache size
   */
  private List<EdgeSourceRow> cache = new ArrayList<>();

  /**
   * Create a new adjacent iterator instance
   *
   * @param scanner accumulo batch scanner
   * @param handler data handler
   * @param cacheSize client result cache size
   */
  public EdgeSourceRowIterator(
    @Nonnull BatchScanner scanner,
    @Nonnull AccumuloIncidentHandler handler,
    int cacheSize
  ) {
    this.scanner = scanner;
    this.handler = handler;
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
        EdgeSourceRow next = handler.readFromEdge(iterator.next());
        cache.add(next);
      }
      return hasNext();

    } else {
      //cache is empty and iterator has no element any more
      return false;

    }
  }

  @Override
  public EdgeSourceRow next() {
    return cache.remove(0);
  }

}
