package org.gradoop;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.config.GradoopAccumuloConfig;
import org.gradoop.flink.io.impl.accumulo.AccumuloDataSinkSourceTest;
import org.gradoop.flink.io.impl.accumulo.AccumuloGraphStoreTest;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/***
 * This file belongs to gradoop project 
 *
 * @author chpengzh
 * @since 2018/2/24
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  AccumuloGraphStoreTest.class, AccumuloDataSinkSourceTest.class
})
public class AccumuloTestSuit {

  private static final Logger LOG = LoggerFactory.getLogger(AccumuloTestSuit.class);
  private static final String ACCUMULO_PASSWORD = "123456";
  private static final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

  private static File tempDirectory;
  private static MiniAccumuloCluster accumulo;

  public static MiniAccumuloCluster getAccumulo() {
    return accumulo;
  }

  public static GradoopAccumuloConfig getAcConfig() {
//    return new GradoopAccumuloConfig.Builder().zookeeper(accumulo.getZooKeepers())
//      .acInstance(accumulo.getInstanceName()).acUser("root")
//      .acPasswd(accumulo.getConfig().getRootPassword()).create();
    return new GradoopAccumuloConfig.Builder()
      .zookeeper("docker2:2181")
      .acInstance("isinonet")
      .acPasswd("123456")
      .acUser("root")
      .create();
  }

  public static GradoopFlinkConfig getFlinkConfig() {
    return GradoopFlinkConfig.createConfig(env);
  }

  @BeforeClass
  public static void setupAccumulo() throws Exception {
    LOG.info("create mini accumulo cluster instance");
    tempDirectory = new File(String.format("/tmp/_accumulo_%d", System.currentTimeMillis()));
    MiniAccumuloConfig config = new MiniAccumuloConfig(tempDirectory, ACCUMULO_PASSWORD);
    config.setNativeLibPaths(AccumuloTestSuit.class.getResource("/").getFile());
    accumulo = new MiniAccumuloCluster(config);
    accumulo.start();
    LOG.info("create mini accumulo start success!");
  }

  @AfterClass
  public static void terminateAccumulo() throws IOException, InterruptedException {
    try {
      LOG.info("terminate mini accumulo cluster");
      accumulo.stop();
    } finally {
      if (tempDirectory != null && tempDirectory.delete()) {
        LOG.info("delete temp dir success !");
      }
    }
  }

}
