package com.twitter.pycascading;

import org.python.util.PythonInterpreter;

/**
 * This is the class that holds the Python environment running on a mapper or
 * reducer, including the Python interpreter.
 * 
 * @author Gabor Szabo
 */
public class PythonEnvironment {
  private PythonInterpreter interpreter;

  /**
   * Start a new Jython interpreter if it's not started yet.
   * 
   * @return the interpreter instance
   */
  public PythonInterpreter getPythonInterpreter() {
    if (interpreter == null)
      interpreter = new PythonInterpreter();
    return interpreter;
  }
}
