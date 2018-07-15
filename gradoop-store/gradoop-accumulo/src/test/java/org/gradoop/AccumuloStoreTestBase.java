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

package org.gradoop;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.common.model.api.entites.EPGMAdjacencyRow;
import org.gradoop.common.model.impl.AdjacencyRow;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.common.util.AsciiGraphLoader;
import org.gradoop.flink.io.impl.accumulo.AccumuloDataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;

import javax.annotation.Nonnull;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AccumuloStoreTestBase extends GradoopFlinkTestBase {

  /**
   * Load social network graph and write it into accumulo graph
   *
   * @param namespace store namespace
   * @param context loader context
   * @throws Throwable if error
   */
  protected void storeImportAndTest(
    String namespace,
    SocialTestContext context
  ) throws Throwable {
    GradoopAccumuloConfig config = AccumuloTestSuite
      .getAcConfig(getExecutionEnvironment(), namespace);
    AccumuloEPGMStore graphStore = new AccumuloEPGMStore(config);

    // read vertices by label
    AsciiGraphLoader<GraphHead, Vertex, Edge> loader = GradoopTestUtils.getSocialNetworkLoader();
    // write social graph to Accumulo
    for (GraphHead g : loader.getGraphHeads()) {
      graphStore.writeGraphHead(g);
    }
    for (Vertex v : loader.getVertices()) {
      graphStore.writeVertex(v);
    }
    for (Edge e : loader.getEdges()) {
      graphStore.writeEdge(e);
    }
    graphStore.flush();

    context.test(loader, graphStore);
  }

  /**
   * Load social network graph and write it into accumulo graph
   *
   * @param namespace store namespace
   * @param context loader context
   * @throws Throwable if error
   */
  protected void sinkImportAndTest(
    String namespace,
    FlinkSocialTestContext context
  ) throws Throwable {
    AccumuloEPGMStore accumuloStore = new AccumuloEPGMStore(AccumuloTestSuite
      .getAcConfig(getExecutionEnvironment(), namespace));

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(
      GradoopFlinkConfig.createConfig(getExecutionEnvironment()));

    InputStream inputStream = getClass().getResourceAsStream(
      GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE);

    loader.initDatabaseFromStream(inputStream);
    new AccumuloDataSink(accumuloStore).write(accumuloStore
      .getConfig()
      .getGraphCollectionFactory()
      .fromCollections(
        loader.getGraphHeads(),
        loader.getVertices(),
        loader.getEdges()));
    getExecutionEnvironment().execute();
    accumuloStore.flush();

    context.test(loader, accumuloStore);
  }

  /**
   * Create random sample
   *
   * @param population sample s
   * @param sampleSize sample size
   * @param <T> list type
   * @return sample list
   */
  protected <T> List<T> sample(
    List<T> population,
    int sampleSize
  ) {
    Random random = new Random(System.currentTimeMillis());
    List<T> ret = new ArrayList<>(sampleSize);
    if (sampleSize > population.size()) {
      throw new IllegalArgumentException(String.format(
        "sample size(=%d) is larger than population size (=%d) ",
        sampleSize, population.size()));
    }

    int i = 0, nLeft = population.size();
    while (sampleSize > 0) {
      int rand = random.nextInt(nLeft);
      if (rand < sampleSize) {
        ret.add(population.get(i));
        sampleSize--;
      }
      nLeft--;
      i++;
    }
    return ret;
  }

  public interface SocialTestContext {

    void test(
      AsciiGraphLoader<GraphHead, Vertex, Edge> loader,
      AccumuloEPGMStore store
    ) throws Throwable;

  }

  public interface FlinkSocialTestContext {

    void test(
      FlinkAsciiGraphLoader loader,
      AccumuloEPGMStore store
    ) throws Throwable;

  }
}
