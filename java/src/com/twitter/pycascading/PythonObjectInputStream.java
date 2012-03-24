package com.twitter.pycascading;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

import org.python.core.PyFunction;
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
    if (obj instanceof PythonFunctionWrapper) {
      // Object resolved = super.resolveObject(obj);
      // System.out.println("*** resolvefunc " + resolved + " " +
      // resolved.getClass());
      return obj;
    } else
      return obj;
    // return super.resolveObject(obj);
    // PythonInterpreter interpreter = Main.getInterpreter();
  }
}
