/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

/**
 * Associates an edge with an business transaction graph.
 * @param <E> edge type
 */
public class SetBtgId<E extends EPGMGraphElement>
  implements JoinFunction<E, Tuple2<GradoopId, GradoopId>, E> {

  @Override
  public E join(E element, Tuple2<GradoopId, GradoopId> mapping) {
    element.setGraphIds(GradoopIdSet.fromExisting(mapping.f1));
    return element;
  }
}
