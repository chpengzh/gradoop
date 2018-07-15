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

package org.gradoop.common.storage.impl.accumulo;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.common.model.api.entites.EPGMAdjacencyRow;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.AdjacencyRow;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.storage.api.EPGMConfigProvider;
import org.gradoop.common.storage.api.EPGMGraphInput;
import org.gradoop.common.storage.api.EPGMGraphPredictableOutput;
import org.gradoop.common.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.common.storage.impl.accumulo.constants.GradoopAccumuloProperty;
import org.gradoop.common.storage.impl.accumulo.handler.AccumuloRowHandler;
import org.gradoop.common.storage.impl.accumulo.iterator.client.AdjacencyIterator;
import org.gradoop.common.storage.impl.accumulo.iterator.client.ClientClosableIterator;
import org.gradoop.common.storage.impl.accumulo.iterator.tserver.GradoopEdgeIterator;
import org.gradoop.common.storage.impl.accumulo.iterator.tserver.GradoopGraphHeadIterator;
import org.gradoop.common.storage.impl.accumulo.iterator.tserver.GradoopVertexIterator;
import org.gradoop.common.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;
import org.gradoop.common.storage.iterator.ClosableIterator;
import org.gradoop.common.storage.iterator.EmptyClosableIterator;
import org.gradoop.common.storage.predicate.query.ElementQuery;
import org.gradoop.common.storage.predicate.query.Query;
import org.gradoop.utils.GradoopAccumuloUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Default Accumulo EPGM graph store that handles reading and writing vertices and
 * graphs from and to Accumulo, It is designed thread-safe.
 * Store contains instances are divided by {@link GradoopAccumuloProperty#ACCUMULO_TABLE_PREFIX}
 *
 * @see EPGMGraphPredictableOutput
 */
public class AccumuloEPGMStore implements
  EPGMConfigProvider<GradoopAccumuloConfig>,
  EPGMGraphInput<EPGMGraphHead, EPGMVertex, EPGMEdge>,
  EPGMGraphPredictableOutput<
    AccumuloElementFilter<GraphHead>,
    AccumuloElementFilter<Vertex>,
    AccumuloElementFilter<Edge>> {

  /**
   * Accumulo epgm store logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(AccumuloEPGMStore.class);

  /**
   * Gradoop accumulo configuration
   */
  private final GradoopAccumuloConfig config;

  /**
   * Accumulo client connector
   */
  private final Connector conn;

  /**
   * Batch writer for epgm graph head table
   */
  private final BatchWriter graphWriter;

  /**
   * Batch writer for epgm vertex table
   */
  private final BatchWriter vertexWriter;

  /**
   * Batch writer for epgm edge table
   */
  private final BatchWriter edgeWriter;

  /**
   * Batch writer for epgm adjacency table
   */
  private final BatchWriter adjacencyWriter;

  /**
   * Auto flush flag, default false
   */
  private volatile boolean autoFlush;

  /**
   * Creates an AccumuloEPGMStore based on the given parameters.
   * Tables with given prefix will be auto-create if not exists
   *
   * @param config                      accumulo store configuration
   * @throws AccumuloSecurityException  for security violations,
   *                                    authentication failures,
   *                                    authorization failures,
   *                                    etc.
   * @throws AccumuloException          generic Accumulo Exception for general accumulo failures.
   */
  public AccumuloEPGMStore(@Nonnull GradoopAccumuloConfig config) throws
    AccumuloSecurityException, AccumuloException {
    this.config = config;
    this.conn = createConnector();
    createTablesIfNotExists();
    try {
      graphWriter = conn.createBatchWriter(getGraphHeadName(), new BatchWriterConfig());
      vertexWriter = conn.createBatchWriter(getVertexTableName(), new BatchWriterConfig());
      edgeWriter = conn.createBatchWriter(getEdgeTableName(), new BatchWriterConfig());
      adjacencyWriter = conn.createBatchWriter(getAdjacencyTableName(), new BatchWriterConfig());
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(e); //should not be here
    }
  }

  /**
   * Create an accumulo client connector with given configuration
   *
   * @return accumulo client connector instance
   * @throws AccumuloSecurityException  for security violations,
   *                                    authentication failures,
   *                                    authorization failures,
   *                                    etc.
   * @throws AccumuloException          generic Accumulo Exception for general accumulo failures.
   */
  public Connector createConnector() throws AccumuloSecurityException, AccumuloException {
    return GradoopAccumuloUtils.createConnector(getConfig().getAccumuloProperties());
  }

  @Override
  public GradoopAccumuloConfig getConfig() {
    return config;
  }

  @Override
  public String getVertexTableName() {
    return config.getVertexTable();
  }

  @Override
  public String getEdgeTableName() {
    return config.getEdgeTable();
  }

  @Override
  public String getGraphHeadName() {
    return config.getGraphHeadTable();
  }

  @Override
  public String getAdjacencyTableName() {
    return config.getAdjacencyTable();
  }

  @Override
  public void writeGraphHead(@Nonnull EPGMGraphHead record) {
    writeRecord(record, graphWriter, config.getGraphHandler());
  }

  @Override
  public void writeVertex(@Nonnull EPGMVertex record) {
    writeRecord(record, vertexWriter, config.getVertexHandler());
  }

  @Override
  public void writeEdge(@Nonnull EPGMEdge record) {
    writeRecord(record, edgeWriter, config.getEdgeHandler());
    writeEdgeOut(record);
    writeEdgeIn(record);
  }

  @Override
  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
  }

  @Override
  public void flush() {
    try {
      graphWriter.flush();
      vertexWriter.flush();
      edgeWriter.flush();
      adjacencyWriter.flush();
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      graphWriter.close();
      vertexWriter.close();
      edgeWriter.close();
      adjacencyWriter.close();
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  @Nullable
  @Override
  public GraphHead readGraph(@Nonnull GradoopId graphId) throws IOException {
    ElementQuery<AccumuloElementFilter<GraphHead>> query = Query
      .elements()
      .fromSets(graphId)
      .noFilter();
    try (ClosableIterator<GraphHead> it = getGraphSpace(query, 1)) {
      return it.hasNext() ? it.next() : null;
    }
  }

  @Nullable
  @Override
  public Vertex readVertex(@Nonnull GradoopId vertexId) throws IOException {
    ElementQuery<AccumuloElementFilter<Vertex>> query = Query
      .elements()
      .fromSets(vertexId)
      .noFilter();
    try (ClosableIterator<Vertex> it = getVertexSpace(query, 1)) {
      return it.hasNext() ? it.next() : null;
    }
  }

  @Nullable
  @Override
  public Edge readEdge(@Nonnull GradoopId edgeId) throws IOException {
    ElementQuery<AccumuloElementFilter<Edge>> query = Query
      .elements()
      .fromSets(edgeId)
      .noFilter();
    try (ClosableIterator<Edge> it = getEdgeSpace(query, 1)) {
      return it.hasNext() ? it.next() : null;
    }
  }

  @Nonnull
  @Override
  public ClosableIterator<GraphHead> getGraphSpace(
    @Nullable ElementQuery<AccumuloElementFilter<GraphHead>> query,
    int cacheSize
  ) throws IOException {
    if (query != null &&
      query.getQueryRanges() != null &&
      query.getQueryRanges().isEmpty()) {
      return new EmptyClosableIterator<>();
    }

    if (query != null) {
      LOG.info(query.toString());
    }

    BatchScanner scanner = createBatchScanner(
      getGraphHeadName(),
      GradoopGraphHeadIterator.class,
      query);
    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
    if (!iterator.hasNext()) {
      return new EmptyClosableIterator<>();
    } else {
      return new ClientClosableIterator<>(scanner,
        new GradoopGraphHeadIterator(),
        config.getGraphHandler(),
        cacheSize);
    }
  }

  @Nonnull
  @Override
  public ClosableIterator<Vertex> getVertexSpace(
    @Nullable ElementQuery<AccumuloElementFilter<Vertex>> query,
    int cacheSize
  ) throws IOException {
    if (query != null &&
      query.getQueryRanges() != null &&
      query.getQueryRanges().isEmpty()) {
      return new EmptyClosableIterator<>();
    }

    if (query != null) {
      LOG.info(query.toString());
    }

    BatchScanner scanner = createBatchScanner(
      getVertexTableName(),
      GradoopVertexIterator.class,
      query);
    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
    if (!iterator.hasNext()) {
      return new EmptyClosableIterator<>();
    } else {
      return new ClientClosableIterator<>(scanner,
        new GradoopVertexIterator(),
        config.getVertexHandler(),
        cacheSize);
    }
  }

  @Nonnull
  @Override
  public ClosableIterator<Edge> getEdgeSpace(
    @Nullable ElementQuery<AccumuloElementFilter<Edge>> query,
    int cacheSize
  ) throws IOException {
    if (query != null &&
      query.getQueryRanges() != null &&
      query.getQueryRanges().isEmpty()) {
      return new EmptyClosableIterator<>();
    }

    if (query != null) {
      LOG.info(query.toString());
    }

    BatchScanner scanner = createBatchScanner(
      getEdgeTableName(),
      GradoopEdgeIterator.class,
      query);
    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
    if (!iterator.hasNext()) {
      return new EmptyClosableIterator<>();
    } else {
      return new ClientClosableIterator<>(scanner,
        new GradoopEdgeIterator(),
        config.getEdgeHandler(),
        cacheSize);
    }
  }

  @Nonnull
  @Override
  public ClosableIterator<AdjacencyRow> adjacentFromVertices(
    @Nonnull GradoopIdSet vertexSeeds,
    boolean fetchEdgeIn,
    boolean fetchEdgeOut
  ) {
    if (vertexSeeds.isEmpty()) {
      return new EmptyClosableIterator<>();
    }

    Authorizations auth = GradoopAccumuloProperty.ACCUMULO_AUTHORIZATIONS
      .get(getConfig().getAccumuloProperties());
    int batchSize = GradoopAccumuloProperty.GRADOOP_BATCH_SCANNER_THREADS
      .get(getConfig().getAccumuloProperties());
    try {
      BatchScanner scanner = conn.createBatchScanner(getAdjacencyTableName(), auth, batchSize);
      List<Range> scanRanges = new ArrayList<>();
      for (GradoopId id : vertexSeeds) {
        if (fetchEdgeOut) {
          scanRanges.add(Range.exact(id.toString(), AccumuloTables.KEY.EDGE_OUT));
        }
        if (fetchEdgeIn) {
          scanRanges.add(Range.exact(id.toString(), AccumuloTables.KEY.EDGE_IN));
        }
      }
      scanner.setRanges(Range.mergeOverlapping(scanRanges));
      return new AdjacencyIterator(scanner,
        getConfig().getAdjacencyHandler(),
        EPGMAdjacencyRow.Strategy.FROM_VERTEX_TO_EDGE,
        batchSize);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  @Override
  public ClosableIterator<AdjacencyRow> adjacentFromEdges(
    @Nonnull GradoopIdSet edgeSeeds,
    boolean fetchEdgeSource,
    boolean fetchEdgeTarget
  ) {
    if (edgeSeeds.isEmpty()) {
      return new EmptyClosableIterator<>();
    }

    final Properties properties = getConfig().getAccumuloProperties();
    Authorizations auth = GradoopAccumuloProperty.ACCUMULO_AUTHORIZATIONS.get(properties);
    int batchSize = GradoopAccumuloProperty.GRADOOP_BATCH_SCANNER_THREADS.get(properties);
    try {
      BatchScanner scanner = conn.createBatchScanner(getEdgeTableName(), auth, batchSize);
      List<Range> scanRanges = new ArrayList<>();
      for (GradoopId id : edgeSeeds) {
        if (fetchEdgeTarget) {
          scanRanges.add(Range.exact(id.toString(), AccumuloTables.KEY.TARGET));
        }
        if (fetchEdgeSource) {
          scanRanges.add(Range.exact(id.toString(), AccumuloTables.KEY.SOURCE));
        }
      }
      scanner.setRanges(Range.mergeOverlapping(scanRanges));
      return new AdjacencyIterator(scanner,
        getConfig().getAdjacencyHandler(),
        EPGMAdjacencyRow.Strategy.FROM_EDGE_TO_VERTEX,
        batchSize);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Write an EPGM Element instance into table
   *
   * @param record  gradoop EPGM element
   * @param writer  accumulo batch writer
   * @param handler accumulo row handler
   * @param <T>     element type
   */
  private <T extends EPGMElement> void writeRecord(
    @Nonnull T record,
    @Nonnull BatchWriter writer,
    @Nonnull AccumuloRowHandler handler
  ) {
    Mutation mutation = new Mutation(record.getId().toString());
    //noinspection unchecked
    mutation = handler.writeRow(mutation, record);
    try {
      writer.addMutation(mutation);
      if (autoFlush) {
        writer.flush();
      }
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create accumulo batch scanner with element predicate
   *
   * @param table  table name
   * @param iterator iterator class
   * @param predicate accumulo predicate
   * @param <T> epgm element type
   * @return batch scanner instance
   * @throws IOException if create fail
   */
  private <T extends EPGMElement> BatchScanner createBatchScanner(
    String table,
    Class<? extends SortedKeyValueIterator<Key, Value>> iterator,
    @Nullable ElementQuery<AccumuloElementFilter<T>> predicate
  ) throws IOException {
    Map<String, String> options = new HashMap<>();
    if (predicate != null && predicate.getFilterPredicate() != null) {
      options.put(AccumuloTables.KEY_PREDICATE, predicate.getFilterPredicate().encode());
    }
    BatchScanner scanner;
    try {
      Authorizations auth = GradoopAccumuloProperty.ACCUMULO_AUTHORIZATIONS
        .get(getConfig().getAccumuloProperties());
      int batchSize = GradoopAccumuloProperty.GRADOOP_BATCH_SCANNER_THREADS
        .get(getConfig().getAccumuloProperties());
      int priority = GradoopAccumuloProperty.GRADOOP_ITERATOR_PRIORITY
        .get(getConfig().getAccumuloProperties());
      scanner = conn.createBatchScanner(table, auth, batchSize);
      scanner.addScanIterator(new IteratorSetting(
        /*iterator priority*/priority,
        /*iterator class*/iterator,
        /*args*/options));

      if (predicate == null ||
        predicate.getQueryRanges() == null) {
        scanner.setRanges(Lists.newArrayList(new Range()));
      } else {
        scanner.setRanges(Range.mergeOverlapping(predicate.getQueryRanges()
          .stream()
          .map(GradoopId::toString)
          .map(Range::exact)
          .collect(Collectors.toList())));
      }
      return scanner;

    } catch (TableNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * Create tables (and their namespaces, if defined by table prefix) if not exists
   *
   * @throws AccumuloSecurityException  for security violations,
   *                                    authentication failures,
   *                                    authorization failures,
   *                                    etc.
   * @throws AccumuloException          generic Accumulo Exception for general accumulo failures.
   */
  private void createTablesIfNotExists() throws AccumuloSecurityException, AccumuloException {
    String prefix = GradoopAccumuloProperty.ACCUMULO_TABLE_PREFIX
      .get(getConfig().getAccumuloProperties());

    if (prefix.contains(".")) {
      String namespace = prefix.substring(0, prefix.indexOf("."));
      try {
        if (!conn.namespaceOperations().exists(namespace)) {
          conn.namespaceOperations().create(namespace);
        }
      } catch (NamespaceExistsException ignore) {
        //ignore if it is exists, maybe create by another process or thread
      }
    }
    for (String table : new String[] {
      getVertexTableName(), getEdgeTableName(), getGraphHeadName(), getAdjacencyTableName()
    }) {
      try {
        if (!conn.tableOperations().exists(table)) {
          conn.tableOperations().create(table);
        }
      } catch (TableExistsException ignore) {
        //ignore if it is exists, maybe create by another process or thread
      }
    }
  }

  /**
   * Write an edge-in link record into vertex table
   *
   * @param record epgm edge record
   */
  private void writeEdgeIn(EPGMEdge record) {
    try {
      Mutation mutation = new Mutation(record.getTargetId().toString());
      mutation = config.getAdjacencyHandler().writeIncomeEdge(mutation, record);
      adjacencyWriter.addMutation(mutation);
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Write an edge-out link record into vertex table
   *
   * @param record epgm edge record
   */
  private void writeEdgeOut(EPGMEdge record) {
    try {
      Mutation mutation = new Mutation(record.getSourceId().toString());
      mutation = config.getAdjacencyHandler().writeOutcomeEdge(mutation, record);
      adjacencyWriter.addMutation(mutation);
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

}
