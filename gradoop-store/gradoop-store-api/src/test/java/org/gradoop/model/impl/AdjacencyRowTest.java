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

package org.gradoop.model.impl;

import org.gradoop.common.model.api.entites.EPGMAdjacencyRow;
import org.gradoop.common.model.impl.AdjacencyRow;
import org.gradoop.common.model.impl.id.GradoopId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class AdjacencyRowTest {

  private final Random random = new Random(System.currentTimeMillis());

  @Test
  public void equalityTest() {
    for (int i = 0; i < 10; i++) {
      AdjacencyRow sample = createNew();
      AdjacencyRow test = new AdjacencyRow(
        GradoopId.fromString(sample.getSeedId().toString()),
        GradoopId.fromString(sample.getAdjacentId().toString()),
        sample.getStrategy());
      Assert.assertEquals(sample, test);
    }
  }

  @Test
  public void compareTest() {
    ArrayList<AdjacencyRow> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(createNew());
    }
    List<AdjacencyRow> first = new ArrayList<>(rows);
    List<AdjacencyRow> second = new ArrayList<>(rows);
    first.sort(AdjacencyRow::compareTo);
    second.sort(AdjacencyRow::compareTo);

    Assert.assertEquals(first.size(), second.size());
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(first.get(i), second.get(i));
    }
  }

  private AdjacencyRow createNew() {
    String seedId = GradoopId.get().toString();
    String adjacentId = GradoopId.get().toString();
    int typeOrdinary = random.nextInt(AdjacencyRow.Strategy.values().length);
    EPGMAdjacencyRow.Strategy strategy = AdjacencyRow.Strategy.values()[typeOrdinary];

    return new AdjacencyRow(
      GradoopId.fromString(seedId),
      GradoopId.fromString(adjacentId),
      strategy);
  }

}
