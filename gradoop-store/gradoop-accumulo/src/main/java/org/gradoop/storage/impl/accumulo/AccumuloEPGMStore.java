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
package org.gradoop.storage.impl.accumulo;

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
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.storage.common.api.EPGMConfigProvider;
import org.gradoop.storage.common.api.EPGMGraphInput;
import org.gradoop.storage.common.api.EPGMGraphPredictableOutput;
import org.gradoop.storage.common.iterator.ClosableIterator;
import org.gradoop.storage.common.iterator.EmptyClosableIterator;
import org.gradoop.storage.common.model.EdgeSourceRow;
import org.gradoop.storage.common.model.VertexSourceRow;
import org.gradoop.storage.common.predicate.query.ElementQuery;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.config.GradoopAccumuloConfig;
import org.gradoop.storage.impl.accumulo.constants.AccumuloTables;
import org.gradoop.storage.impl.accumulo.constants.GradoopAccumuloProperty;
import org.gradoop.storage.impl.accumulo.handler.AccumuloIncidentHandler;
import org.gradoop.storage.impl.accumulo.handler.AccumuloRowHandler;
import org.gradoop.storage.impl.accumulo.iterator.client.ClientClosableIterator;
import org.gradoop.storage.impl.accumulo.iterator.client.EdgeSourceRowIterator;
import org.gradoop.storage.impl.accumulo.iterator.client.VertexSourceRowIterator;
import org.gradoop.storage.impl.accumulo.iterator.tserver.GradoopEdgeIterator;
import org.gradoop.storage.impl.accumulo.iterator.tserver.GradoopGraphHeadIterator;
import org.gradoop.storage.impl.accumulo.iterator.tserver.GradoopVertexIterator;
import org.gradoop.storage.impl.accumulo.predicate.filter.api.AccumuloElementFilter;
import org.gradoop.storage.utils.GradoopAccumuloUtils;
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
  EPGMGraphInput,
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
   * Batch writer for incident table
   */
  private final BatchWriter incidentWriter;

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
  public AccumuloEPGMStore(@Nonnull GradoopAccumuloConfig config)
    throws AccumuloSecurityException, AccumuloException {
    this.config = config;
    this.conn = createConnector();
    createTablesIfNotExists();
    try {
      graphWriter = conn.createBatchWriter(getGraphHeadName(), new BatchWriterConfig());
      vertexWriter = conn.createBatchWriter(getVertexTableName(), new BatchWriterConfig());
      edgeWriter = conn.createBatchWriter(getEdgeTableName(), new BatchWriterConfig());
      incidentWriter = conn.createBatchWriter(getIncidentTableName(), new BatchWriterConfig());
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
  public String getIncidentTableName() {
    return config.getIncidentTable();
  }

  @Override
  public void writeGraphHead(@Nonnull EPGMGraphHead record) throws IOException {
    writeRecord(record, graphWriter, config.getGraphHandler());
  }

  @Override
  public void writeVertex(@Nonnull EPGMVertex record) throws IOException {
    writeRecord(record, vertexWriter, config.getVertexHandler());
  }

  @Override
  public void writeEdge(@Nonnull EPGMEdge record) throws IOException {
    writeRecord(record, edgeWriter, config.getEdgeHandler());
    writeIncome(record, incidentWriter, config.getIncidentHandler());
    writeOutcome(record, incidentWriter, config.getIncidentHandler());
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
      incidentWriter.flush();
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      flush();
      graphWriter.close();
      vertexWriter.close();
      edgeWriter.close();
      incidentWriter.close();
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
  public ClosableIterator<VertexSourceRow> getEdgeIdsFromVertexIds(
    @Nonnull GradoopIdSet vertexSeeds,
    @Nonnull VertexSourceRow.Strategy strategy
  ) throws IOException {
    if (vertexSeeds.isEmpty()) {
      return new EmptyClosableIterator<>();
    }

    Authorizations auth = GradoopAccumuloProperty.ACCUMULO_AUTHORIZATIONS
      .get(getConfig().getAccumuloProperties());
    int batchSize = GradoopAccumuloProperty.GRADOOP_BATCH_SCANNER_THREADS
      .get(getConfig().getAccumuloProperties());
    try {
      BatchScanner scanner = conn.createBatchScanner(getIncidentTableName(), auth, batchSize);
      List<Range> scanRanges = new ArrayList<>();
      for (GradoopId id : vertexSeeds) {
        switch (strategy) {
        case INCOME:
          scanRanges.add(Range.exact(id.toString(), AccumuloTables.KEY.EDGE_IN));
          break;
        case OUTCOME:
          scanRanges.add(Range.exact(id.toString(), AccumuloTables.KEY.EDGE_OUT));
          break;
        case BOTH:
          scanRanges.add(Range.exact(id.toString(), AccumuloTables.KEY.EDGE_IN));
          scanRanges.add(Range.exact(id.toString(), AccumuloTables.KEY.EDGE_OUT));
          break;
        default:
          break;
        }
      }

      if (!scanRanges.isEmpty()) {
        scanner.setRanges(Range.mergeOverlapping(scanRanges));
        return new VertexSourceRowIterator(scanner,
          getConfig().getIncidentHandler(),
          batchSize);
      } else {
        return new EmptyClosableIterator<>();
      }

    } catch (TableNotFoundException e) {
      throw new IOException(e);
    }
  }

  @Nonnull
  @Override
  public ClosableIterator<EdgeSourceRow> getVertexIdsFromEdgeIds(
    @Nonnull GradoopIdSet edgeSeeds,
    @Nonnull EdgeSourceRow.Strategy strategy
  ) throws IOException {
    if (edgeSeeds.isEmpty()) {
      return new EmptyClosableIterator<>();
    }

    Authorizations auth = GradoopAccumuloProperty.ACCUMULO_AUTHORIZATIONS
      .get(getConfig().getAccumuloProperties());
    int batchSize = GradoopAccumuloProperty.GRADOOP_BATCH_SCANNER_THREADS
      .get(getConfig().getAccumuloProperties());
    try {
      BatchScanner scanner = conn.createBatchScanner(getEdgeTableName(), auth, batchSize);
      List<Range> scanRanges = new ArrayList<>();
      for (GradoopId id : edgeSeeds) {
        switch (strategy) {
        case SOURCE:
          scanRanges.add(Range.exact(id.toString(), AccumuloTables.KEY.SOURCE));
          break;
        case TARGET:
          scanRanges.add(Range.exact(id.toString(), AccumuloTables.KEY.TARGET));
          break;
        case BOTH:
          scanRanges.add(Range.exact(id.toString(), AccumuloTables.KEY.SOURCE));
          scanRanges.add(Range.exact(id.toString(), AccumuloTables.KEY.TARGET));
          break;
        default:
          break;
        }
      }

      if (!scanRanges.isEmpty()) {
        scanner.setRanges(Range.mergeOverlapping(scanRanges));
        return new EdgeSourceRowIterator(scanner,
          getConfig().getIncidentHandler(),
          batchSize);
      } else {
        return new EmptyClosableIterator<>();
      }

    } catch (TableNotFoundException e) {
      throw new IOException(e);
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
    @Nonnull AccumuloRowHandler<?, T> handler
  ) throws IOException {
    Mutation mutation = new Mutation(record.getId().toString());
    mutation = handler.writeRow(mutation, record);
    enqueueMutation(writer, mutation);
  }

  /**
   * Write external incident relation index ({vertex-id, income, edge-id})
   *
   * @param record  gradoop EPGM element
   * @param writer  accumulo batch writer
   * @param handler accumulo row handler
   */
  private void writeIncome(
    @Nonnull EPGMEdge record,
    @Nonnull BatchWriter writer,
    @Nonnull AccumuloIncidentHandler handler
  ) throws IOException {
    Mutation mutation = new Mutation(record.getTargetId().toString());
    mutation = handler.writeIncomeEdge(mutation, record);
    enqueueMutation(writer, mutation);
  }

  /**
   * Write external incident relation index ({vertex-id, outcome, edge-id})
   *
   * @param record  gradoop EPGM element
   * @param writer  accumulo batch writer
   * @param handler accumulo row handler
   */
  private void writeOutcome(
    @Nonnull EPGMEdge record,
    @Nonnull BatchWriter writer,
    @Nonnull AccumuloIncidentHandler handler
  ) throws IOException {
    Mutation mutation = new Mutation(record.getSourceId().toString());
    mutation = handler.writeOutcomeEdge(mutation, record);
    enqueueMutation(writer, mutation);
  }

  /**
   * Enqueue mutation execution
   *
   * @param writer batch writer instance
   * @param mutation mutation to be execute
   * @throws IOException if mutation is rejected by remote
   */
  private void enqueueMutation(
    @Nonnull BatchWriter writer,
    @Nonnull Mutation mutation
  ) throws IOException {
    try {
      writer.addMutation(mutation);
      if (autoFlush) {
        writer.flush();
      }
    } catch (MutationsRejectedException e) {
      throw new IOException(e);
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
      getVertexTableName(), getEdgeTableName(), getGraphHeadName(), getIncidentTableName()
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

}
