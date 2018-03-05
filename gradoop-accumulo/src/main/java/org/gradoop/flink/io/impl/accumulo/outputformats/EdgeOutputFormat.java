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

package org.gradoop.flink.io.impl.accumulo.outputformats;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * edge output format
 */
public class EdgeOutputFormat extends AbstractAccumuloOutputFormat<Edge> {

  /**
   * edge output format constructor
   *
   * @param config gradoop accumulo config
   */
  public EdgeOutputFormat(GradoopAccumuloConfig config) {
    super(config);
  }

  @Override
  protected BatchWriter getWriter(Connector conn) throws TableNotFoundException {
    return conn.createBatchWriter("gradoop.edge", new BatchWriterConfig());
  }

  @Override
  protected void write(BatchWriter writer, Edge record) throws MutationsRejectedException {
    Mutation mutation = new Mutation(record.getId().toString());
    mutation.put("label", "", record.getLabel());
    mutation.put("source", "", record.getSourceId().toString());
    mutation.put("target", "", record.getTargetId().toString());
    Iterable<String> keys = record.getPropertyKeys();
    if (keys != null) {
      keys.forEach(key -> {
        String value = record.getPropertyValue(key).toString();
        mutation.put("property", key, value);
      });
    }
    GradoopIdSet ids = record.getGraphIds();
    if (ids != null) {
      ids.forEach(id -> {
        mutation.put("graph", id.toString(), "");
      });
    }
    writer.addMutation(mutation);
  }
}
