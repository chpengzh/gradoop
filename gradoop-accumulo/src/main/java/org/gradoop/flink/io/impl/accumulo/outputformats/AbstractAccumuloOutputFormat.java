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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.common.model.impl.pojo.Element;

import java.io.IOException;

/**
 * abstract output format for accumulo data sink
 * @param <T> element define in gradoop
 */
public abstract class AbstractAccumuloOutputFormat<T extends Element> implements OutputFormat<T> {

  /**
   * batch writer for output into accumulo table
   */
  private transient BatchWriter writer;

  /**
   * gradoop accumulo configure
   */
  private final GradoopAccumuloConfig config;

  /**
   *
   * @param config gradoop accumulo config
   */
  AbstractAccumuloOutputFormat(GradoopAccumuloConfig config) {
    this.config = config;
  }

  @Override
  public void configure(Configuration parameters) {
  }

  /**
   * create batch writer
   *
   * @param conn accumulo connector
   * @return batch write
   * @throws TableNotFoundException if table is not exists
   */
  protected abstract BatchWriter getWriter(Connector conn) throws TableNotFoundException;

  /**
   *
   * @param writer batch writer
   * @param record element record
   */
  protected abstract void write(BatchWriter writer, T record) throws MutationsRejectedException;

  @Override
  public final void open(int taskNumber, int numTasks) throws IOException {
    try {
      //create connector for accumulo client
      writer = getWriter(
        new ZooKeeperInstance(config.getAccumuloInstance(), config.getZookeeperHosts())
          .getConnector(config.getAccumuloUser(), new PasswordToken(config.getAccumuloPasswd())));
    } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
      throw new IOException(e);
    }
  }

  @Override
  public final void writeRecord(T record) throws IOException {
    try {
      write(writer, record);
    } catch (MutationsRejectedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public final void close() throws IOException {
    if (writer != null) {
      try {
        writer.close();
      } catch (MutationsRejectedException e) {
        throw new IOException(e);
      }
    }
  }

}
