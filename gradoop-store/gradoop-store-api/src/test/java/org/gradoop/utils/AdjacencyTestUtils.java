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

package org.gradoop.utils;

import org.gradoop.common.model.api.entites.EPGMAdjacencyRow;
import org.gradoop.common.model.impl.AdjacencyRow;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AdjacencyTestUtils {

  public static void validateAdjacency(
    @Nonnull List<AdjacencyRow> collection1,
    @Nonnull List<AdjacencyRow> collection2,
    @Nonnull EPGMAdjacencyRow.Strategy strategy
  ) {
    assertNotNull("first collection was null", collection1);
    assertNotNull("second collection was null", collection1);
    assertEquals(String.format("collections of different size: %d and %d",
      collection1.size(),
      collection2.size()),
      collection1.size(),
      collection2.size());

    List<AdjacencyRow> list1 = new ArrayList<>(collection1);
    List<AdjacencyRow> list2 = new ArrayList<>(collection2);

    list1.sort(AdjacencyRow::compareTo);
    list2.sort(AdjacencyRow::compareTo);

    Iterator<AdjacencyRow> it1 = list1.iterator();
    Iterator<AdjacencyRow> it2 = list2.iterator();

    while (it1.hasNext()) {
      AdjacencyRow a = it1.next();
      AdjacencyRow b = it2.next();
      assertEquals("id mismatch", a.getSeedId(), b.getSeedId());
      assertEquals("id mismatch", a.getAdjacentId(), b.getAdjacentId());
      assertEquals("strategy mismatch", a.getStrategy(), strategy);
      assertEquals("strategy mismatch", b.getStrategy(), strategy);
    }
  }

}
