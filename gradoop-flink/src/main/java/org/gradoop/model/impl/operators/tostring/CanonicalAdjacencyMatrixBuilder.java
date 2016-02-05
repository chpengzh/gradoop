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

package org.gradoop.model.impl.operators.tostring;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.functions.epgm.LabelCombiner;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.model.impl.operators.tostring.api.VertexToString;
import org.gradoop.model.impl.operators.tostring.functions.AdjacencyMatrix;
import org.gradoop.model.impl.operators.tostring.functions.ConcatGraphHeadStrings;
import org.gradoop.model.impl.operators.tostring.functions.IncomingAdjacencyList;
import org.gradoop.model.impl.operators.tostring.functions.MultiEdgeStringCombiner;
import org.gradoop.model.impl.operators.tostring.functions.OutgoingAdjacencyList;
import org.gradoop.model.impl.operators.tostring.functions.SourceStringUpdater;
import org.gradoop.model.impl.operators.tostring.functions.TargetStringUpdater;
import org.gradoop.model.impl.operators.tostring.tuples.EdgeString;
import org.gradoop.model.impl.operators.tostring.tuples.GraphHeadString;
import org.gradoop.model.impl.operators.tostring.tuples.VertexString;

/**
 * Operator deriving a string representation from a graph collection.
 * The representation follows the concept of a canonical adjacency matrix.
 * @param <G> graph type
 * @param <V> vertex type
 * @param <E> edge type
 */
public class CanonicalAdjacencyMatrixBuilder
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphCollectionToValueOperator<G, V, E, String> {

  /**
   * function describing string representation of graph heads
   */
  private final GraphHeadToString<G> graphHeadToString;
  /**
   * function describing string representation of vertices
   */
  private final VertexToString<V> vertexToString;
  /**
   * function describing string representation of edges
   */
  private final EdgeToString<E> egeLabelingFunction;

  /**
   * constructor
   * @param graphHeadToString representation of graph heads
   * @param vertexToString representation of vertices
   * @param egeLabelingFunction representation of edges
   */
  public CanonicalAdjacencyMatrixBuilder(
    GraphHeadToString<G> graphHeadToString,
    VertexToString<V> vertexToString,
    EdgeToString<E> egeLabelingFunction) {
    this.graphHeadToString = graphHeadToString;
    this.vertexToString = vertexToString;
    this.egeLabelingFunction = egeLabelingFunction;
  }

  @Override
  public DataSet<String> execute(GraphCollection<G, V, E> collection) {

    // 1. label graph heads
    DataSet<GraphHeadString> graphHeadLabels = collection.getGraphHeads()
      .map(graphHeadToString);

    // 2. label vertices
    DataSet<VertexString> vertexLabels = collection.getVertices()
      .flatMap(vertexToString);

    // 3. label edges
    DataSet<EdgeString> edgeLabels = collection.getEdges()
      .flatMap(egeLabelingFunction)
      .groupBy(0, 1, 2)
      .reduceGroup(new MultiEdgeStringCombiner());

    // 4. extend edge labels by vertex labels

    edgeLabels = edgeLabels
      .join(vertexLabels)
      .where(0, 1).equalTo(0, 1) // graphId,sourceId = graphId,vertexId
      .with(new SourceStringUpdater())
      .join(vertexLabels)
      .where(0, 2).equalTo(0, 1) // graphId,targetId = graphId,vertexId
      .with(new TargetStringUpdater());

    // 5. extend vertex labels by outgoing vertex+edge labels

    DataSet<VertexString> outgoingAdjacencyListLabels = edgeLabels
      .groupBy(0, 1) // graphId, sourceId
      .reduceGroup(new OutgoingAdjacencyList());


    // 6. extend vertex labels by outgoing vertex+edge labels

    DataSet<VertexString> incomingAdjacencyListLabels = edgeLabels
      .groupBy(0, 2) // graphId, targetId
      .reduceGroup(new IncomingAdjacencyList());

    // 7. combine vertex labels

    vertexLabels = vertexLabels
      .leftOuterJoin(outgoingAdjacencyListLabels)
      .where(0, 1).equalTo(0, 1)
      .with(new LabelCombiner<VertexString>())
      .leftOuterJoin(incomingAdjacencyListLabels)
      .where(0, 1).equalTo(0, 1)
      .with(new LabelCombiner<VertexString>());

    // 8. create adjacency matrix labels

    DataSet<GraphHeadString> adjacencyMatrixLabels = vertexLabels
      .groupBy(0)
      .reduceGroup(new AdjacencyMatrix());

    // 9. combine graph labels

    graphHeadLabels = graphHeadLabels
      .join(adjacencyMatrixLabels)
      .where(0).equalTo(0)
      .with(new LabelCombiner<GraphHeadString>());

    // 10. add empty head to prevent empty result for empty collection

    graphHeadLabels = graphHeadLabels
      .union(collection
        .getConfig()
        .getExecutionEnvironment()
        .fromElements(new GraphHeadString(GradoopId.get(), "")));

    // 11. label collection

    return graphHeadLabels
      .reduceGroup(new ConcatGraphHeadStrings());
  }
}
