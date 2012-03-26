package com.twitter.pycascading;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

import org.apache.hadoop.mapred.JobConf;
import org.python.core.Py;
import org.python.core.PyFunction;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;

public class PythonObjectInputStream extends ObjectInputStream {

  private InputStream inputStream;
  private StringBuilder sources;

  public PythonObjectInputStream(InputStream in, StringBuilder sources) throws IOException {
    super(in);
    inputStream = in;
    this.sources = sources;
    enableResolveObject(true);
  }

  @Override
  protected Object resolveObject(Object obj) throws IOException {
    // System.out.println("*** resolve " + obj + " " + obj.getClass());
    // JobConf conf = new JobConf();
    // System.out.println("***** jobconf " +
    // conf.get("mapred.cache.localArchives"));
    if (obj instanceof PythonFunctionWrapper) {
      PythonInterpreter interpreter = Main.getInterpreter();
      String name = Py.tojava(((PythonFunctionWrapper) obj).funcName, String.class);
      System.out.println("#### resolved " + name + " " + obj + " " + obj.getClass());
      // String expr = name + " = lambda: 1";
      // interpreter.exec(expr);
      interpreter.exec(((PythonFunctionWrapper) obj).funcSource);
      // System.out.println("**** execed " + expr);
      PyObject ret = interpreter.get(name);
      System.out.println("*** returned " + ret + "/" + ret.getClass());
      // Object resolved = super.resolveObject(obj);
      // System.out.println("*** resolvefunc " + resolved + " " +
      // resolved.getClass());
      return ret;
    } else
      return obj;
    // return super.resolveObject(obj);
    // PythonInterpreter interpreter = Main.getInterpreter();
  }
}
