package com.twitter.pycascading;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

import org.python.core.Py;
import org.python.core.PyObject;
import org.python.core.PyTuple;
import org.python.util.PythonInterpreter;

/**
 * When deserializing the job, this class reconstructs the Python functions
 * given by their name and/or source.
 * 
 * @author Gabor Szabo
 */
public class PythonObjectInputStream extends ObjectInputStream {

  private PythonInterpreter interpreter;

  public PythonObjectInputStream(InputStream in, PythonInterpreter interpreter) throws IOException {
    super(in);
    this.interpreter = interpreter;
    enableResolveObject(true);
  }

  @Override
  protected Object resolveObject(Object obj) throws IOException {
    // This method will reconstruct the PyFunction based on its name or its
    // source if it was a closure
    if (obj instanceof SerializedPythonFunction) {
      PyTuple serializedFunction = ((SerializedPythonFunction) obj).getSerializedFunction();
      String functionType = (String) serializedFunction.get(0);
      String functionName = (String) serializedFunction.get(3);
      PyObject function = null;
      if ("global".equals(functionType)) {
        function = interpreter.get(functionName);
      } else if ("closure".equals(functionType)) {
        interpreter.exec((String) serializedFunction.get(4));
        function = interpreter.get(functionName);
      }
      return function;
    } else
      return obj;
  }
}
