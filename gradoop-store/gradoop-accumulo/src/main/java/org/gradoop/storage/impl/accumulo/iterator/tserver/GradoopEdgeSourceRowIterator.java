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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.gradoop.storage.common.model.EdgeSourceRow;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Gradoop edge source row iterator
 */
public class GradoopEdgeSourceRowIterator extends BaseSourceRowIterator {

  @Override
  boolean shouldFetch(
    @Nonnull String strategy,
    @Nonnull Pair<Key, Value> next
  ) {
    switch (next.getFirst().getColumnFamily().toString()) {
    case AccumuloTables.KEY.SOURCE:
      return strategy.isEmpty() ||
        Objects.equals(strategy, EdgeSourceRow.Strategy.BOTH.toString()) ||
        Objects.equals(strategy, EdgeSourceRow.Strategy.SOURCE.toString());
    case AccumuloTables.KEY.TARGET:
      return strategy.isEmpty() ||
        Objects.equals(strategy, EdgeSourceRow.Strategy.BOTH.toString()) ||
        Objects.equals(strategy, EdgeSourceRow.Strategy.TARGET.toString());
    default:
      return false;
    }
  }

}
