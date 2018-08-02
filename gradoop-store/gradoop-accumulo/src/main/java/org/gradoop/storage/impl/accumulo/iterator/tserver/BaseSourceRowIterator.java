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

package org.gradoop.storage.impl.accumulo.iterator.tserver;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.Pair;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Abstract source row iterator
 */
public abstract class BaseSourceRowIterator implements SortedKeyValueIterator<Key, Value> {

  /**
   * Origin accumulo data source
   */
  private SortedKeyValueIterator<Key, Value> source;

  /**
   * Serialized top row
   */
  private Pair<Key, Value> top;

  /**
   * strategy value
   */
  private String strategy;

  /**
   * Judge if this row should be return from store precess
   *
   * @param strategy query strategy option
   * @param next next element
   * @return return flag
   */
  abstract boolean shouldFetch(
    @Nonnull String strategy,
    @Nonnull Pair<Key, Value> next
  );

  @Override
  public final void init(
    SortedKeyValueIterator<Key, Value> source,
    Map<String, String> options,
    IteratorEnvironment env
  ) throws IOException {
    this.source = source;
    //read filter predicate
    if (options != null && !options.isEmpty() &&
      options.containsKey(AccumuloTables.KEY_SOURCE_ROW_STRATEGY)) {
      this.strategy = options.get(AccumuloTables.KEY_SOURCE_ROW_STRATEGY);
    } else {
      this.strategy = "";
    }
  }

  @Override
  public boolean hasTop() {
    return this.top != null;
  }

  @Override
  public void next() throws IOException {
    Pair<Key, Value> next;
    do {
      try {
        next = source.hasTop() ? new Pair<>(source.getTopKey(), source.getTopValue()) : null;
        if (next != null && !shouldFetch(strategy, next)) {
          next = null;
        }
        if (source.hasTop()) {
          source.next();
        }
      } catch (IOException err) {
        throw new RuntimeException(err);
      }
    } while (source.hasTop() && next == null);
    top = next;
  }

  @Override
  public void seek(
    Range range,
    Collection<ByteSequence> columnFamilies,
    boolean inclusive
  ) throws IOException {
    source.seek(range, columnFamilies, inclusive);
    next();
  }

  @Override
  public Key getTopKey() {
    return top == null ? null : top.getFirst();
  }

  @Override
  public Value getTopValue() {
    return top == null ? null : top.getSecond();
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException("deep copy is not supported!");
  }

}
