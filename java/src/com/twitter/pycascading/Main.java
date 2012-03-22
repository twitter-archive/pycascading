package com.twitter.pycascading;

import java.util.Properties;

import org.python.util.PythonInterpreter;

public class Main {

  private static PythonInterpreter interpreter;

  public static void main(String[] args) throws Exception {
    Properties sysProps = System.getProperties();
    Properties props = new Properties();
    props.put("python.cachedir", sysProps.get("user.home") + "/.jython-cache");
    props.put("python.cachedir.skip", "0");
    PythonInterpreter.initialize(System.getProperties(), props, args);
    interpreter = new PythonInterpreter();
    System.out.println("****** int: " + interpreter);
    interpreter.execfile(args[0]);
  }

  public static PythonInterpreter getInterpreter() {
    return interpreter;
  }
}
