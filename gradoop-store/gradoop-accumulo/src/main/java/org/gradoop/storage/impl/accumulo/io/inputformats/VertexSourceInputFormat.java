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
import org.gradoop.storage.common.model.VertexSourceRow;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.storage.impl.accumulo.handler.AccumuloIncidentHandler;
import org.gradoop.storage.impl.accumulo.iterator.tserver.GradoopVertexSourceRowIterator;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Vertex Source Row InputFormat
 */
public class VertexSourceInputFormat extends BaseSourceRowInputFormat<VertexSourceRow> {

  /**
   * Incident encoding/decoding handler
   */
  private final AccumuloIncidentHandler handler = new AccumuloIncidentHandler();

  /**
   * Query strategy
   */
  private final VertexSourceRow.Strategy strategy;

  /**
   * Create a new vertex input format
   *
   * @param properties accumulo properties
   * @param strategy query strategy
   */
  public VertexSourceInputFormat(
    @Nonnull Properties properties,
    @Nonnull VertexSourceRow.Strategy strategy
  ) {
    super(properties);
    this.strategy = strategy;
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  Iterator<Map.Entry<Key, Value>> openIterator(
    @Nonnull BatchScanner scanner,
    @Nonnull Range range,
    int iteratorPriority
  ) {
    Map<String, String> options = Maps.newHashMap(
      AccumuloTables.KEY_SOURCE_ROW_STRATEGY, strategy.toString());
    scanner.addScanIterator(new IteratorSetting(iteratorPriority,
      GradoopVertexSourceRowIterator.class, options));
    scanner.setRanges(Lists.newArrayList(range));
    return scanner.iterator();
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  String getTable(@Nonnull String tablePrefix) {
    return String.format("%s%s", tablePrefix, AccumuloTables.INCIDENT);
  }

  /**
   * {@inheritDoc}
   */
  @Nonnull
  @Override
  VertexSourceRow next(@Nonnull Map.Entry<Key, Value> entry) {
    return handler.readFromVertex(entry);
  }

}
