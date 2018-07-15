/*
 * Copyright © Since 2018 www.isinonet.com Company
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

package org.gradoop.common.storage.iterator;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Closeable iterator test
 */
public class ClosableIteratorTest {

  /**
   * Create an anonymous iterator and test its state and reading
   */
  @Test
  public void createIteratorAndClose() throws IOException {
    boolean[] isClose = {false}; //close flag
    final List<Integer> data = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7); //test data

    //test iterator
    ClosableIterator<Integer> iterator = new ClosableIterator<Integer>() {
      private final Iterator<Integer> inner = data.iterator();

      @Override
      public void close() throws IOException {
        isClose[0] = true;
      }

      @Override
      public boolean hasNext() {
        return inner.hasNext();
      }

      @Override
      public Integer next() {
        return inner.next();
      }
    };

    //read one element and do not close
    List<Integer> query = new ArrayList<>(iterator.read(1));
    Assert.assertEquals(1, query.size());
    Assert.assertTrue(!isClose[0]);

    //read all remains and close
    query.addAll(iterator.readRemainsAndClose());
    Assert.assertEquals(data.size(), query.size());
    Assert.assertTrue(isClose[0]);
    for (int i = 0; i < data.size(); i++) {
      Assert.assertEquals(data.get(i), query.get(i));
    }
  }

  /**
   * Create an empty iterator
   */
  @Test
  public void createEmptyIterator() throws IOException {
    try (ClosableIterator<Boolean> iterator = new EmptyClosableIterator<>()) {
      Assert.assertTrue(!iterator.hasNext());
      Assert.assertTrue(iterator.read(Integer.MAX_VALUE).isEmpty());
    }
  }

}
