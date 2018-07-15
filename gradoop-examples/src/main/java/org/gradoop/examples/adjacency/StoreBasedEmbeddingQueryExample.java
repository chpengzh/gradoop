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

package org.gradoop.examples.adjacency;

import com.google.common.io.Files;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.common.model.impl.AdjacencyRow;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.common.storage.impl.accumulo.constants.GradoopAccumuloProperty;
import org.gradoop.common.storage.predicate.query.Query;
import org.gradoop.common.utils.AccumuloFilters;
import org.gradoop.flink.io.impl.accumulo.AccumuloDataSource;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.impl.functions.adjacency.AdjacentId;
import org.gradoop.flink.model.impl.functions.adjacency.SeedId;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Store Based embedding query
 */
public class StoreBasedEmbeddingQueryExample {

  /**
   * Logger instance
   */
  private static final Logger LOG = LoggerFactory.getLogger(StoreBasedEmbeddingQueryExample.class);

  /**
   * Path of the input graph.
   */
  private static final String DATA_PATH =
    StoreBasedEmbeddingQueryExample.class.getResource("/data/json/sna").getFile();

  /**
   * Single embedding query as below:
   * MATCH (sub)-[rel]->(obj) WHERE sub.name=\"Alice\"
   * It is just a hard code example, using store-based adjacency query
   *
   * @param args runtime args
   */
  public static void main(String... args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
    AccumuloEPGMStore store = createStore(env);
    AccumuloDataSource source = new AccumuloDataSource(store);

    DataSet<Vertex> subjects = source
      .applyVertexPredicate(
        Query.elements()
          .fromAll()
          .where(AccumuloFilters.propEquals("name", "Alice")))
      .getGraphCollection()
      .getVertices();
    DataSet<AdjacencyRow> subjectAdjacency = source.adjacentFromVertices(
      subjects.map(new Id<>()),
      false,
      true);
    //TODO: let query range support DataSet but id set
    DataSet<Edge> relations = source
      .applyEdgePredicate(Query.elements()
        .fromSets(GradoopIdSet.fromExisting(
          subjectAdjacency
            .map(new AdjacentId<>())
            .distinct()
            .collect()))
        .noFilter())
      .getGraphCollection()
      .getEdges();
    DataSet<AdjacencyRow> relAdjacency = source.adjacentFromEdges(
      relations.map(new Id<>()),
      false,
      true);
    DataSet<Vertex> objects = source
      .applyVertexPredicate(
        Query.elements()
          .fromSets(GradoopIdSet.fromExisting(relAdjacency
            .map(new AdjacentId<>())
            .distinct()
            .collect()))
          .noFilter())
      .getGraphCollection()
      .getVertices();

    //generate result graph with adjacency rows
    //returns {sub(id)->rel(id), rel(id)->obj(id)}
    subjectAdjacency
      .join(relAdjacency)
      .where(new AdjacentId<>())
      .equalTo(new SeedId<>())
      .returns(new TypeHint<Tuple2<AdjacencyRow, AdjacencyRow>>() {
      })
      .filter((FilterFunction<Tuple2<AdjacencyRow, AdjacencyRow>>) it -> it.f1 != null)

      // join subject definition
      // return {{sub(id)->rel(id), rel(id)->obj(id)}, {sub}}
      .join(subjects)
      .where(new KeySelector<Tuple2<AdjacencyRow, AdjacencyRow>, GradoopId>() {
        @Override
        public GradoopId getKey(Tuple2<AdjacencyRow, AdjacencyRow> it) throws Exception {
          return it.f0.getSeedId();
        }
      })
      .equalTo(new Id<>())
      .with((JoinFunction<
        Tuple2<AdjacencyRow, AdjacencyRow>,
        Vertex,
        Tuple2<Tuple2<AdjacencyRow, AdjacencyRow>, Tuple1<Vertex>>>) (it, sub) ->
        new Tuple2<>(it, new Tuple1<>(sub)))
      .returns(new TypeHint<Tuple2<Tuple2<AdjacencyRow, AdjacencyRow>, Tuple1<Vertex>>>() {
      })
      .filter((FilterFunction<Tuple2<Tuple2<AdjacencyRow, AdjacencyRow>, Tuple1<Vertex>>>) it ->
        it.f1.f0 != null)

      // join relation definition
      // return {{sub(id)->rel(id), rel(id)->obj(id)}, {sub, rel}}
      .join(relations)
      .where(new KeySelector<
        Tuple2<Tuple2<AdjacencyRow, AdjacencyRow>, Tuple1<Vertex>>,
        GradoopId>() {
        @Override
        public GradoopId getKey(
          Tuple2<Tuple2<AdjacencyRow, AdjacencyRow>, Tuple1<Vertex>> it
        ) throws Exception {
          return it.f0.f0.getAdjacentId();
        }
      })
      .equalTo(new Id<>())
      .with((JoinFunction<
        Tuple2<Tuple2<AdjacencyRow, AdjacencyRow>, Tuple1<Vertex>>,
        Edge,
        Tuple2<Tuple2<AdjacencyRow, AdjacencyRow>, Tuple2<Vertex, Edge>>>) (it, rel) ->
        new Tuple2<>(it.f0, new Tuple2<>(it.f1.f0, rel)))
      .returns(new TypeHint<Tuple2<Tuple2<AdjacencyRow, AdjacencyRow>, Tuple2<Vertex, Edge>>>() {
      })
      .filter((FilterFunction<Tuple2<Tuple2<AdjacencyRow, AdjacencyRow>,
        Tuple2<Vertex, Edge>>>) it -> it.f1.f1 != null)

      // join object definition
      // return {sub, rel, obj}
      .join(objects)
      .where(new KeySelector<
        Tuple2<Tuple2<AdjacencyRow, AdjacencyRow>, Tuple2<Vertex, Edge>>,
        GradoopId>() {
        @Override
        public GradoopId getKey(
          Tuple2<Tuple2<AdjacencyRow, AdjacencyRow>, Tuple2<Vertex, Edge>> it
        ) throws Exception {
          return it.f0.f1.getAdjacentId();
        }
      })
      .equalTo(new Id<>())
      .with((JoinFunction<
        Tuple2<Tuple2<AdjacencyRow, AdjacencyRow>, Tuple2<Vertex, Edge>>,
        Vertex,
        Tuple3<Vertex, Edge, Vertex>>) (it, obj) ->
        new Tuple3<>(it.f1.f0, it.f1.f1, obj))
      .returns(new TypeHint<Tuple3<Vertex, Edge, Vertex>>() {
      })
      .filter((FilterFunction<Tuple3<Vertex, Edge, Vertex>>) it -> it.f2 != null)
      .print();
  }

  /**
   * Create a new accumulo store instance
   *
   * @param env flink environment
   * @return accumulo store instance
   */
  private static AccumuloEPGMStore createStore(ExecutionEnvironment env) throws Exception {
    GradoopAccumuloConfig config = setupAccumulo(env);
    AccumuloEPGMStore store = new AccumuloEPGMStore(config);
    JSONDataSource source = new JSONDataSource(DATA_PATH, config);
    GraphCollection collection = source.getGraphCollection();
    for (GraphHead head : collection.getGraphHeads().collect()) {
      store.writeGraphHead(head);
    }
    for (Vertex vertex : collection.getVertices().collect()) {
      store.writeVertex(vertex);
    }
    for (Edge edge : collection.getEdges().collect()) {
      store.writeEdge(edge);
    }
    store.flush();
    return store;
  }

  /**
   * Create a new accumulo mini-cluster for testing
   *
   * @param env flink environment
   * @return accumulo configuration
   */
  private static GradoopAccumuloConfig setupAccumulo(ExecutionEnvironment env) throws Exception {
    File tmp = Files.createTempDir();
    tmp.deleteOnExit();
    MiniAccumuloConfig config = new MiniAccumuloConfig(tmp, "123456");
    config.setNativeLibPaths(StoreBasedEmbeddingQueryExample.class
      .getResource("/runtime")
      .getFile());
    MiniAccumuloCluster accumulo = new MiniAccumuloCluster(config);
    accumulo.start();
    LOG.info("create mini accumulo start success!");
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          accumulo.stop();
        } catch (IOException | InterruptedException ignore) {
        } finally {
          LOG.info("create mini accumulo terminate!");
        }
      }
    }));
    return GradoopAccumuloConfig.getDefaultConfig(env)
      .set(GradoopAccumuloProperty.ACCUMULO_USER, "root")
      .set(GradoopAccumuloProperty.ACCUMULO_INSTANCE, accumulo.getInstanceName())
      .set(GradoopAccumuloProperty.ZOOKEEPER_HOSTS, accumulo.getZooKeepers())
      .set(GradoopAccumuloProperty.ACCUMULO_PASSWD, accumulo.getConfig().getRootPassword())
      .set(GradoopAccumuloProperty.ACCUMULO_TABLE_PREFIX, "test");
  }


}
