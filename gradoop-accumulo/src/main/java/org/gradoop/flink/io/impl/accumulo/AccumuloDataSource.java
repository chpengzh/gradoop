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

package org.gradoop.flink.io.impl.accumulo;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.accumulo.inputformats.EdgeInputFormat;
import org.gradoop.flink.io.impl.accumulo.inputformats.GraphHeadInputFormat;
import org.gradoop.flink.io.impl.accumulo.inputformats.VertexInputFormat;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Accumulo DataSource
 */
public class AccumuloDataSource implements DataSource {

  /**
   * gradoop accumulo configure
   */
  private final GradoopAccumuloConfig accumuloConfig;

  /**
   * gradoop flink configure
   */
  private final GradoopFlinkConfig flinkConfig;

  /**
   * constructor for accumulo data source
   * @param flinkConfig flink configure
   * @param accumuloConfig accumulo configure
   */
  public AccumuloDataSource(GradoopFlinkConfig flinkConfig, GradoopAccumuloConfig accumuloConfig) {
    this.flinkConfig = flinkConfig;
    this.accumuloConfig = accumuloConfig;
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() {
    GraphCollectionFactory factory = flinkConfig.getGraphCollectionFactory();
    ExecutionEnvironment env = flinkConfig.getExecutionEnvironment();
    return factory.fromDataSets(env.createInput(new GraphHeadInputFormat(accumuloConfig)),
      env.createInput(new VertexInputFormat(accumuloConfig)),
      env.createInput(new EdgeInputFormat(accumuloConfig)));
  }

}
