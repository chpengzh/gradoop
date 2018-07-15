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

package org.gradoop.flink.io.impl.accumulo.outputformats;

import org.apache.accumulo.core.data.Mutation;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloAdjacencyHandler;

import java.util.Properties;

/**
 * Edge-Out OutputFormat
 */
public class EdgeOutOutputFormat extends BaseOutputFormat<Edge> {

  /**
   * serialize id
   */
  private static final int serialVersionUID = 0x1;

  /**
   * vertex handler
   */
  private transient AccumuloAdjacencyHandler handler;

  /**
   * Create a new output format for gradoop edge-out
   *
   * @param properties accumulo properties
   */
  public EdgeOutOutputFormat(Properties properties) {
    super(properties);
  }

  @Override
  protected void initiate() {
    handler = new AccumuloAdjacencyHandler();
  }

  @Override
  protected String getTableName(String prefix) {
    return String.format("%s%s", prefix, AccumuloTables.ADJACENCY);
  }

  @Override
  protected Mutation writeMutation(Edge record) {
    Mutation mutation = new Mutation(record.getSourceId().toString());
    mutation = handler.writeOutcomeEdge(mutation, record);
    return mutation;
  }

}
