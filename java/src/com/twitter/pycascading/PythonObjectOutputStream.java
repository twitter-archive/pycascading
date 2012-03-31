package com.twitter.pycascading;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.python.core.Py;
import org.python.core.PyFunction;
import org.python.core.PyNone;
import org.python.core.PyObject;
import org.python.core.PyTuple;

/**
 * This class replaces every function object with a pointer to its name and/or
 * source, so that we can reconstruct the function when deserializing. We need
 * to do it this way as PyFunctions cannot be serialized (some nested Jython
 * objects don't implement Serializable).
 * 
 * @author Gabor Szabo
 */
public class PythonObjectOutputStream extends ObjectOutputStream {

  private PyFunction callBack;

  public PythonObjectOutputStream(OutputStream out, PyFunction callBack) throws IOException {
    super(out);
    this.callBack = callBack;
    enableReplaceObject(true);
  }

  @Override
  protected Object replaceObject(Object obj) throws IOException {
    if (obj instanceof PyFunction) {
      PyObject replaced = callBack.__call__((PyObject) obj);
      if (!(replaced instanceof PyNone)) {
        System.out.println("######## replaced " + obj + "/" + obj.getClass() + "->" + replaced
                + " " + replaced.getClass());
        System.out.println("******* " + ((PyTuple) replaced).get(0).getClass());
        return new SerializedPythonFunction((PyFunction) obj, (PyTuple) replaced);
      }
    }
    return obj;
  }
}
