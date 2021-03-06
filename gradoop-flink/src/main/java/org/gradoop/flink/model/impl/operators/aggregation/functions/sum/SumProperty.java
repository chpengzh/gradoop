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
package org.gradoop.flink.model.impl.operators.aggregation.functions.sum;

import org.gradoop.flink.model.api.functions.AggregateFunction;

/**
 * Superclass if aggregate functions that sum property values of vertices OR
 * edges.
 */
public abstract class SumProperty extends Sum implements AggregateFunction {

  /**
   * Property key whose value should be aggregated.
   */
  protected final String propertyKey;

  /**
   * Constructor.
   *
   * @param propertyKey property key to aggregate
   */
  public SumProperty(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public String getAggregatePropertyKey() {
    return "sum_" + propertyKey;
  }
}
