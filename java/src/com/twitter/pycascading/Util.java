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

import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;

import org.python.util.PythonInterpreter;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

/**
 * Helper cass that sets up the MR environment and runs a Cascading Flow.
 * 
 * @author Gabor Szabo
 */
public class Util {
  // http://www.velocityreviews.com/forums/t147526-how-to-get-jar-file-name.html
  public static String getJarFolder() {
    try {
      return cascading.pipe.Pipe.class.getProtectionDomain().getCodeSource().getLocation().toURI()
              .getPath();
    } catch (URISyntaxException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  public static void run(int numReducers, Map<String, Object> config, Map<String, Tap> sources,
          Map<String, Tap> sinks, Pipe... tails) {
    // String strClassPath = System.getProperty("java.class.path");
    // System.out.println("Classpath is " + strClassPath);

    Properties properties = new Properties();
    properties.put("mapred.reduce.tasks", numReducers);
    // Set this to change the default block size that is routed to one mapper
    // It won't help if the files are smaller than this as each file will go to
    // one mapper
    // properties.put("mapred.min.split.size", 20 * 1024 * 1024 * 1024L);
    // properties.put("mapred.map.tasks", 4000);
    // So that Thrift classes can be serialized
    properties.put("io.serializations",
            "com.twitter.pycascading.bigintegerserialization.BigIntegerSerialization");
    properties.put("mapred.jobtracker.completeuserjobs.maximum", 50000);
    properties.put("mapred.input.dir.recursive", "true");
    FlowConnector.setApplicationJarClass(properties, Util.class);

    FlowConnector flowConnector = new FlowConnector(properties);
    try {
      Flow flow = flowConnector.connect(sources, sinks, tails);
      // execute the flow, block until complete
      flow.complete();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String args[]) {
    Properties sysProps = System.getProperties();
    Properties props = new Properties();
    props.put("python.cachedir", sysProps.get("user.home") + "/.jython-cache");
    props.put("python.cachedir.skip", "0");
    PythonInterpreter.initialize(System.getProperties(), props, args);
    PythonInterpreter interpreter = new PythonInterpreter();
    interpreter.execfile(getJarFolder() + args[0]);
  }
}
