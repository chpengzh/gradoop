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

package org.gradoop.storage.impl.accumulo.index;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.storage.impl.accumulo.AccumuloStoreTestBase;
import org.gradoop.storage.impl.accumulo.io.AccumuloIndexedDataSource;
import org.gradoop.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;
import org.gradoop.storage.utils.AccumuloFilters;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GraphIndexTest extends AccumuloStoreTestBase {

  private static final String TEST01 = "graph_index_test_01";
  private static final String TEST02 = "graph_index_test_02";

  /**
   * Do insert by store, and query all graph that fulfill filter predicate
   *
   * @throws Throwable if test error
   */
  @Test
  public void test01_doInsertAndQueryWithPredicate() throws Throwable {
    storeInsertAndTest(TEST01, (loader, store, config) -> {
      AccumuloElementFilter<GraphHead> filter = AccumuloFilters.labelIn("Community");

      List<GraphHead> graphs = new ArrayList<>(loader.getGraphHeads())
        .stream()
        .filter(filter)
        .collect(Collectors.toList());

      AccumuloIndexedDataSource dataSource = new AccumuloIndexedDataSource(config, store);
      List<GraphHead> query = dataSource.getGraphHeads(filter)
        .collect();

      GradoopTestUtils.validateEPGMElementCollections(graphs, query);
    });
  }

  /**
   * Do insert by store, pick some graph head seeds,
   * query graph head within id seeds that fulfill filter predicate
   *
   * @throws Throwable if test error
   */
  @Test
  public void test02_doInsertAndQueryWithIds() throws Throwable {
    storeInsertAndTest(TEST02, (loader, store, config) -> {
      AccumuloElementFilter<GraphHead> filter = AccumuloFilters.labelIn("Community");
      for (int i = 0; i < 10; i++) {
        List<GraphHead> graphs = sample(new ArrayList<>(loader.getGraphHeads())
          .stream()
          .filter(filter)
          .collect(Collectors.toList()), 3);
        List<GradoopId> ids = graphs
          .stream()
          .map(Element::getId)
          .collect(Collectors.toList());

        AccumuloIndexedDataSource dataSource = new AccumuloIndexedDataSource(config, store);
        List<GraphHead> query = dataSource
          .getGraphHeads(getExecutionEnvironment().fromCollection(ids), filter)
          .collect();

        GradoopTestUtils.validateEPGMElementCollections(graphs, query);
      }
    });
  }

}
