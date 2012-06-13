/**
 * Copyright 2011 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.pycascading;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.FlowListener;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

/**
 * Helper class that sets up the MR environment and runs a Cascading Flow.
 * 
 * @author Gabor Szabo
 */
public class Util {
  // http://www.velocityreviews.com/forums/t147526-how-to-get-jar-file-name.html
  /**
   * Get the temporary folder where the job jar was extracted to by Hadoop.
   * 
   * TODO: This only works if we distribute PyCascading as classes. If I switch
   * to using jars, I need to remove the last part of the path which is the jar
   * file.
   * 
   * @return the temporary folder with the contents of the job jar
   */
  public static String getJarFolder() {
    try {
      return Util.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Could not get temporary job folder");
    }
  }

  /**
   * Get the Cascading jar file on the local file system.
   * 
   * @return the file location on the Hadoop worker for the Cascading jar
   */
  public static List<String> getCascadingJar() {
    try {
      List<String> jars = new ArrayList<String>();
      jars.add(cascading.pipe.Pipe.class.getProtectionDomain().getCodeSource().getLocation()
              .toURI().getPath());
      // In Cascading 2.0 the Hadoop-specific classes are in a different jar
      jars.add(cascading.tap.hadoop.Hfs.class.getProtectionDomain().getCodeSource().getLocation()
              .toURI().getPath());
      return jars;
    } catch (URISyntaxException e) {
      throw new RuntimeException("Could not get the location of the Cascading jars");
    }
  }

  /**
   * We use the "pycascading.root" Java system property to store the location of
   * the Python sources for PyCascading. This is only used in local mode. This
   * is needed so that we know where to set the import path when we start up the
   * mappers and reducers.
   * 
   * @param root
   *          the location of the PyCascading sources on the local file system
   */
  public static void setPycascadingRoot(String root) {
    System.setProperty("pycascading.root", root);
  }

  public static void run(int numReducers, Map<String, Object> config, Map<String, Tap> sources,
          Map<String, Tap> sinks, Pipe... tails) throws IOException, URISyntaxException {
    // String strClassPath = System.getProperty("java.class.path");
    // System.out.println("Classpath is " + strClassPath);

    Properties properties = new Properties();
    // In Cascading 2.0 we need to use strings
    properties.put("mapred.reduce.tasks", new Integer(numReducers).toString());
    // Set this to change the default block size that is routed to one mapper
    // It won't help if the files are smaller than this as each file will go to
    // one mapper
    // properties.put("mapred.min.split.size", 20 * 1024 * 1024 * 1024L);
    // properties.put("mapred.map.tasks", 4000);
    // So that Thrift classes can be serialized
    // We need to add WritableSerialization otherwise sometimes Cascading and
    // Hadoop don't pick it up, and BigInteger serializations fail
    // See https://github.com/twitter/pycascading/issues/2
    // TODO: find the reason for this
    properties.put("io.serializations",
            "com.twitter.pycascading.bigintegerserialization.BigIntegerSerialization,"
                    + "org.apache.hadoop.io.serializer.WritableSerialization,"
                    + "com.twitter.pycascading.pythonserialization.PythonSerialization");
    properties.put("mapred.jobtracker.completeuserjobs.maximum", "50000");
    properties.put("mapred.input.dir.recursive", "true");
    properties.put("elephantbird.mapred.input.bad.record.min", "8");

    // Set the running mode in the jobconf so that the mappers/reducers can
    // easily check this.
    String runningMode = (String) config.get("pycascading.running_mode");
    properties.setProperty("pycascading.running_mode", runningMode);
    properties.setProperty("pycascading.main_file", (String) config.get("pycascading.main_file"));

    Configuration conf = new Configuration();
    TemporaryHdfs tempDir = null;
    if ("hadoop".equals(runningMode)) {
      tempDir = new TemporaryHdfs();
      // We put the files to be distributed into the distributed cache
      // The pycascading.distributed_cache.archives variable was set by
      // bootstrap.py, based on the command line parameters where we specified
      // the PyCascading & source archives
      Object archives = config.get("pycascading.distributed_cache.archives");
      if (archives != null) {
        tempDir = new TemporaryHdfs();
        String tempDirLocation = tempDir.createTmpFolder(conf);
        String dests = null;
        for (String archive : (Iterable<String>) archives) {
          String dest = tempDir.copyFromLocalFileToHDFS(archive);
          dests = (dests == null ? dest : dests + "," + dest);
        }
        // Set the distributed cache to the files we just copied to HDFS
        //
        // This is an ugly hack, we should use DistributedCache.
        // DistributedCache however operates on a JobConf, and since
        // Cascading expects a Map, we cannot directly pass
        // in the parameters set into a JobConf.
        // TODO: see if a later version of Cascading can update its properties
        // using a JobConf
        properties.setProperty("mapred.cache.archives", dests);
        // This creates a symlink for each of the mappers/reducers to the
        // localized files, instead of copying them for each one. This way we
        // reduce the overhead for copying on one worker machine.
        // TODO: see the one just above
        properties.setProperty("mapred.create.symlink", "yes");
      }
    }

    FlowConnector.setApplicationJarClass(properties, Main.class);
    FlowConnector flowConnector = new HadoopFlowConnector(properties);
    Flow flow = flowConnector.connect(sources, sinks, tails);
    if ("hadoop".equals(runningMode)) {
      try {
        flow.addListener(tempDir);
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      try {
        flow.addListener(new FlowListener() {

          @Override
          public void onStarting(Flow flow) {
          }

          @Override
          public void onStopping(Flow flow) {
          }

          @Override
          public void onCompleted(Flow flow) {
          }

          @Override
          public boolean onThrowable(Flow flow, Throwable throwable) {
            throwable.printStackTrace();
            return false;
          }
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    flow.complete();
  }
}
