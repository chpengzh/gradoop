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

package org.gradoop.flink.io.impl.accumulo.inputformats;

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.common.model.impl.pojo.Element;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.io.IOException;

/**
 * abstract input format for accumulo data sink
 * @param <T> element define in gradoop
 */
public abstract class AbstractAccumuloInputFormat<T extends Element> extends GenericInputFormat<T> {

  /**
   * declare a serial version uid for Serializable
   */
  private static final long serialVersionUID = 0x0L;

  /**
   * gradoop accumulo configure
   */
  private final GradoopAccumuloConfig config;

  /**
   *
   * @param config gradoop accumulo config
   */
  AbstractAccumuloInputFormat(GradoopAccumuloConfig config) {
    this.config = config;
  }

  @Override
  public void configure(Configuration parameters) {
  }

  @Override
  @OverridingMethodsMustInvokeSuper
  public void open(GenericInputSplit split) throws IOException {
    super.open(split);
  }

  @Override
  @OverridingMethodsMustInvokeSuper
  public void close() throws IOException {
  }
}
