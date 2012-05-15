package com.twitter.pycascading;

import java.util.Properties;

import org.python.util.PythonInterpreter;

public class Main {

  private static PythonInterpreter interpreter = null;

  /**
   * This is the main method that gets passed to Hadoop, or executed in local
   * mode.
   * 
   * @param args
   *          the command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Properties sysProps = System.getProperties();
    Properties props = new Properties();
    props.put("python.cachedir", sysProps.get("user.home") + "/.jython-cache");
    props.put("python.cachedir.skip", "0");
    PythonInterpreter.initialize(System.getProperties(), props, args);
    getInterpreter().execfile(args[0]);
  }

  /**
   * Create and return the Python interpreter (singleton per JVM).
   * 
   * @return the Python interpreter
   */
  public static PythonInterpreter getInterpreter() {
    if (interpreter == null) {
      interpreter = new PythonInterpreter();
    }
    return interpreter;
  }
}
