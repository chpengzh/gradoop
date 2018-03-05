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

import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.accumulo.outputformats.EdgeOutputFormat;
import org.gradoop.flink.io.impl.accumulo.outputformats.GraphHeadOutputFormat;
import org.gradoop.flink.io.impl.accumulo.outputformats.VertexOutputFormat;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Accumulo DataSink
 */
public class AccumuloDataSink implements DataSink {


  /**
   * gradoop accumulo configure
   */
  private final GradoopAccumuloConfig accumuloConfig;

  /**
   * gradoop flink configure
   */
  private final GradoopFlinkConfig flinkConfig;

  /**
   * constructor for accumulo data sink
   *
   * @param flinkConfig gradoop flink configure
   * @param accumuloConfig gradoop accumulo configure
   */
  public AccumuloDataSink(GradoopFlinkConfig flinkConfig, GradoopAccumuloConfig accumuloConfig) {
    this.flinkConfig = flinkConfig;
    this.accumuloConfig = accumuloConfig;
  }

  @Override
  public void write(LogicalGraph logicalGraph) {
    write(logicalGraph, false);
  }

  @Override
  public void write(GraphCollection graphCollection) {
    write(graphCollection, false);
  }

  @Override
  public void write(LogicalGraph logicalGraph, boolean overwrite) {
    write(flinkConfig.getGraphCollectionFactory().fromGraph(logicalGraph), overwrite);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overWrite) {
    graphCollection.getGraphHeads().output(new GraphHeadOutputFormat(accumuloConfig));
    graphCollection.getVertices().output(new VertexOutputFormat(accumuloConfig));
    graphCollection.getEdges().output(new EdgeOutputFormat(accumuloConfig));
  }

}
