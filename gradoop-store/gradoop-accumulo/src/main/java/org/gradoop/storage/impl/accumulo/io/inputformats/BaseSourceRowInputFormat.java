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

package org.gradoop.storage.impl.accumulo.io.inputformats;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.gradoop.storage.impl.accumulo.constants.GradoopAccumuloProperty;
import org.gradoop.storage.utils.GradoopAccumuloUtils;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Common Abstract {@link InputFormat} for gradoop source row
 *
 * @param <T> source row type
 */
public abstract class BaseSourceRowInputFormat<T> extends GenericInputFormat<T> {

  /**
   * Accumulo properties
   */
  private final Properties properties;

  /**
   * Accumulo batch scanner
   */
  private transient BatchScanner scanner;

  /**
   * Accumulo key-value iterator
   */
  private transient Iterator<Map.Entry<Key, Value>> it;

  /**
   * Create a new source row input format
   *
   * @param properties accumulo properties
   */
  BaseSourceRowInputFormat(@Nonnull Properties properties) {
    this.properties = properties;
  }

  @Override
  public final void open(GenericInputSplit split) throws IOException {
    super.open(split);

    String tableName = getTable(GradoopAccumuloProperty.ACCUMULO_TABLE_PREFIX.get(properties));
    Authorizations auth = GradoopAccumuloProperty.ACCUMULO_AUTHORIZATIONS.get(properties);
    int batchSize = GradoopAccumuloProperty.GRADOOP_BATCH_SCANNER_THREADS.get(properties);
    int priority = GradoopAccumuloProperty.GRADOOP_ITERATOR_PRIORITY.get(properties);

    try {
      Connector conn = GradoopAccumuloUtils.createConnector(properties);
      List<Range> ranges = GradoopAccumuloUtils.getSplits(
        split.getTotalNumberOfSplits(),
        tableName,
        properties,
        null);
      if (split.getSplitNumber() < ranges.size()) {
        scanner = conn.createBatchScanner(tableName, auth, batchSize);
        it = openIterator(scanner, ranges.get(split.getSplitNumber()), priority);
      } else {
        scanner = null;
        it = new ArrayList<Map.Entry<Key, Value>>().iterator();
      }
    } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
      throw new IOException(e);
    }
  }

  @Override
  public final void close() {
    if (this.scanner != null) {
      this.scanner.close();
    }
  }

  @Override
  public final boolean reachedEnd() {
    return !it.hasNext();
  }

  @Override
  public final T nextRecord(T t) {
    return next(it.next());
  }

  /**
   * Open result iterator by given strategy
   *
   * @param scanner accumulo batch scanner
   * @param range query range
   * @param iteratorPriority priority
   * @return iterator instance
   */
  @Nonnull
  abstract Iterator<Map.Entry<Key, Value>> openIterator(
    @Nonnull BatchScanner scanner,
    @Nonnull Range range,
    int iteratorPriority
  );

  /**
   * Get query table name by given table prefix
   *
   * @param tablePrefix table prefix
   * @return table name
   */
  @Nonnull
  abstract String getTable(@Nonnull String tablePrefix);

  /**
   * Map entry to next element
   *
   * @param entry entry pair content
   * @return next element instance
   */
  @Nonnull
  abstract T next(@Nonnull Map.Entry<Key, Value> entry);

}
