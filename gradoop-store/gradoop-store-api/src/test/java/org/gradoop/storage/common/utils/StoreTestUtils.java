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

package org.gradoop.storage.common.utils;

import org.gradoop.storage.common.model.EdgeSourceRow;
import org.gradoop.storage.common.model.VertexSourceRow;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StoreTestUtils {

  public static void validateVertexSourceRow(
    @Nonnull List<VertexSourceRow> collection1,
    @Nonnull List<VertexSourceRow> collection2
  ) {
    assertNotNull("first collection was null", collection1);
    assertNotNull("second collection was null", collection1);
    assertEquals(String.format("collections of different size: %d and %d",
      collection1.size(),
      collection2.size()),
      collection1.size(),
      collection2.size());

    List<VertexSourceRow> list1 = new ArrayList<>(collection1);
    List<VertexSourceRow> list2 = new ArrayList<>(collection2);

    list1.sort(VertexSourceRow::compareTo);
    list2.sort(VertexSourceRow::compareTo);

    Iterator<VertexSourceRow> it1 = list1.iterator();
    Iterator<VertexSourceRow> it2 = list2.iterator();

    while (it1.hasNext()) {
      VertexSourceRow a = it1.next();
      VertexSourceRow b = it2.next();
      assertEquals("id mismatch", a.getSourceVertexId(), b.getSourceVertexId());
      assertEquals("id mismatch", a.getIsolatedEdgeId(), b.getIsolatedEdgeId());
      assertEquals("strategy mismatch", a.getStrategy(), b.getStrategy());
    }
  }

  public static void validateEdgeSourceRow(
    @Nonnull List<EdgeSourceRow> collection1,
    @Nonnull List<EdgeSourceRow> collection2
  ) {
    assertNotNull("first collection was null", collection1);
    assertNotNull("second collection was null", collection1);
    assertEquals(String.format("collections of different size: %d and %d",
      collection1.size(),
      collection2.size()),
      collection1.size(),
      collection2.size());

    List<EdgeSourceRow> list1 = new ArrayList<>(collection1);
    List<EdgeSourceRow> list2 = new ArrayList<>(collection2);

    list1.sort(EdgeSourceRow::compareTo);
    list2.sort(EdgeSourceRow::compareTo);

    Iterator<EdgeSourceRow> it1 = list1.iterator();
    Iterator<EdgeSourceRow> it2 = list2.iterator();

    while (it1.hasNext()) {
      EdgeSourceRow a = it1.next();
      EdgeSourceRow b = it2.next();
      assertEquals("id mismatch", a.getSourceEdgeId(), b.getSourceEdgeId());
      assertEquals("id mismatch", a.getIsolatedVertexId(), b.getIsolatedVertexId());
      assertEquals("strategy mismatch", a.getStrategy(), b.getStrategy());
    }
  }

}
