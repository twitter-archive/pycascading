package com.twitter.pycascading;

import java.util.Properties;

import org.python.util.PythonInterpreter;

public class Main {

  private static PythonInterpreter interpreter = null;

  public static void main(String[] args) throws Exception {
    Properties sysProps = System.getProperties();
    Properties props = new Properties();
    props.put("python.cachedir", sysProps.get("user.home") + "/.jython-cache");
    props.put("python.cachedir.skip", "0");
    PythonInterpreter.initialize(System.getProperties(), props, args);
    getInterpreter().execfile(args[0]);
  }

  public static PythonInterpreter getInterpreter() {
    if (interpreter == null)
      interpreter = new PythonInterpreter();
    return interpreter;
  }
}
