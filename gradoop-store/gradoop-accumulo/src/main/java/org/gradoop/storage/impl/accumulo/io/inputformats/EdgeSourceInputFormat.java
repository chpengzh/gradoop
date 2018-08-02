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

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.gradoop.storage.common.model.EdgeSourceRow;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.storage.impl.accumulo.handler.AccumuloIncidentHandler;
import org.gradoop.storage.impl.accumulo.iterator.tserver.GradoopEdgeSourceRowIterator;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Edge Source Row InputFormat
 */
public class EdgeSourceInputFormat extends BaseSourceRowInputFormat<EdgeSourceRow> {

  /**
   * Incident encoding/decoding handler
   */
  private final AccumuloIncidentHandler handler = new AccumuloIncidentHandler();

  /**
   * Query strategy
   */
  private final EdgeSourceRow.Strategy strategy;

  /**
   * Create a new edge source input format
   *
   * @param properties accumulo properties
   * @param strategy query strategy
   */
  public EdgeSourceInputFormat(
    @Nonnull Properties properties,
    @Nonnull EdgeSourceRow.Strategy strategy
  ) {
    super(properties);
    this.strategy = strategy;
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  Iterator<Map.Entry<Key, Value>> openIterator(
    @Nonnull BatchScanner scanner,
    @Nonnull Range range,
    int iteratorPriority
  ) {
    Map<String, String> options = Maps.newHashMap(
      AccumuloTables.KEY_SOURCE_ROW_STRATEGY, strategy.toString());
    scanner.addScanIterator(new IteratorSetting(iteratorPriority,
      GradoopEdgeSourceRowIterator.class, options));
    scanner.setRanges(Lists.newArrayList(range));
    return scanner.iterator();
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  String getTable(@Nonnull String tablePrefix) {
    return String.format("%s%s", tablePrefix, AccumuloTables.EDGE);
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  EdgeSourceRow next(@Nonnull Map.Entry<Key, Value> entry) {
    return handler.readFromEdge(entry);
  }

}
