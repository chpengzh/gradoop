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
package org.gradoop.common.config;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.storage.config.GradoopStoreConfig;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.common.storage.impl.accumulo.constants.GradoopAccumuloProperty;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloAdjacencyHandler;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloEdgeHandler;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloGraphHandler;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloVertexHandler;

import javax.annotation.Nonnull;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Gradoop Accumulo configuration define
 */
public class GradoopAccumuloConfig extends
  GradoopStoreConfig<GraphHeadFactory, VertexFactory, EdgeFactory> {

  /**
   * define for serialize version control
   */
  private static final int serialVersionUID = 23;

  /**
   * accumulo properties
   */
  private final Properties accumuloProperties = new Properties();

  /**
   * row handler for EPGM GraphHead
   */
  private transient AccumuloGraphHandler graphHandler;

  /**
   * row handler for EPGM Vertex
   */
  private transient AccumuloVertexHandler vertexHandler;

  /**
   * row handler for EPGM Edge
   */
  private transient AccumuloEdgeHandler edgeHandler;

  /**
   * Row handler for adjacency
   */
  private transient AccumuloAdjacencyHandler adjacencyHandler;

  /**
   * Creates a new Configuration.
   *
   * @param graphHandler                graph head handler
   * @param vertexHandler               vertex handler
   * @param edgeHandler                 edge handler
   * @param adjacencyHandler            adjacent handler
   * @param env                         flink execution environment
   */
  private GradoopAccumuloConfig(
    AccumuloGraphHandler graphHandler,
    AccumuloVertexHandler vertexHandler,
    AccumuloEdgeHandler edgeHandler,
    AccumuloAdjacencyHandler adjacencyHandler,
    ExecutionEnvironment env
  ) {
    super(new GraphHeadFactory(), new VertexFactory(), new EdgeFactory(), env);
    this.graphHandler = graphHandler;
    this.vertexHandler = vertexHandler;
    this.edgeHandler = edgeHandler;
    this.adjacencyHandler = adjacencyHandler;
  }

  /**
   * Creates a new Configuration.
   *
   * @param config Gradoop configuration
   */
  private GradoopAccumuloConfig(GradoopAccumuloConfig config) {
    this(config.graphHandler,
      config.vertexHandler,
      config.edgeHandler,
      config.adjacencyHandler,
      config.getExecutionEnvironment());
    this.accumuloProperties.putAll(config.accumuloProperties);
  }

  /**
   * Creates a default Configuration using POJO handlers for vertices, edges
   * and graph heads and default table names.
   *
   * @param env apache flink execution environment
   * @return Default Gradoop Accumulo configuration.
   */
  public static GradoopAccumuloConfig getDefaultConfig(
    ExecutionEnvironment env
  ) {
    GraphHeadFactory graphHeadFactory = new GraphHeadFactory();
    EdgeFactory edgeFactory = new EdgeFactory();
    VertexFactory vertexFactory = new VertexFactory();
    return new GradoopAccumuloConfig(
      new AccumuloGraphHandler(graphHeadFactory),
      new AccumuloVertexHandler(vertexFactory),
      new AccumuloEdgeHandler(edgeFactory),
      new AccumuloAdjacencyHandler(),
      env);
  }

  /**
   * Creates a Gradoop Accumulo configuration based on the given arguments.
   *
   * @param gradoopConfig   Gradoop configuration
   * @return Gradoop HBase configuration
   */
  public static GradoopAccumuloConfig createConfig(@Nonnull GradoopAccumuloConfig gradoopConfig) {
    return new GradoopAccumuloConfig(gradoopConfig);
  }

  /**
   * property setter
   *
   * @param key property key
   * @param value property value
   * @return configure itself
   */
  public GradoopAccumuloConfig set(
    GradoopAccumuloProperty key,
    Object value
  ) {
    accumuloProperties.put(key.getKey(), value);
    return this;
  }

  /**
   * integer value by key
   *
   * @param key property key
   * @param defValue default value
   * @param <T> value template
   * @return integer value
   */
  public <T> T get(
    String key,
    T defValue
  ) {
    Object value = accumuloProperties.get(key);
    if (value == null) {
      return defValue;
    } else {
      //noinspection unchecked
      return (T) value;
    }
  }

  public Properties getAccumuloProperties() {
    return accumuloProperties;
  }

  public AccumuloGraphHandler getGraphHandler() {
    return graphHandler;
  }

  public AccumuloVertexHandler getVertexHandler() {
    return vertexHandler;
  }

  public AccumuloEdgeHandler getEdgeHandler() {
    return edgeHandler;
  }

  public AccumuloAdjacencyHandler getAdjacencyHandler() {
    return adjacencyHandler;
  }

  /**
   * Get edge table name
   *
   * @return edge table name
   */
  public String getEdgeTable() {
    return String.format("%s%s",
      GradoopAccumuloProperty.ACCUMULO_TABLE_PREFIX.get(accumuloProperties),
      AccumuloTables.EDGE);
  }

  /**
   * Get vertex table name
   *
   * @return vertex table name
   */
  public String getVertexTable() {
    return String.format("%s%s",
      GradoopAccumuloProperty.ACCUMULO_TABLE_PREFIX.get(accumuloProperties),
      AccumuloTables.VERTEX);
  }

  /**
   * Get graph head table name
   *
   * @return graph head table name
   */
  public String getGraphHeadTable() {
    return String.format("%s%s",
      GradoopAccumuloProperty.ACCUMULO_TABLE_PREFIX.get(accumuloProperties),
      AccumuloTables.GRAPH);
  }

  /**
   * Get adjacency table name
   *
   * @return graph head table name
   */
  public String getAdjacencyTable() {
    return String.format("%s%s",
      GradoopAccumuloProperty.ACCUMULO_TABLE_PREFIX.get(accumuloProperties),
      AccumuloTables.ADJACENCY);
  }

  @Override
  public String toString() {
    return Stream.of(GradoopAccumuloProperty.values())
      .collect(Collectors.toMap(
        GradoopAccumuloProperty::getKey,
        it -> it.get(accumuloProperties)))
      .toString();
  }

}
