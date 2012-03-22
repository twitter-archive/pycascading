package com.twitter.pycascading;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public class PythonObjectInputStream extends ObjectInputStream {

  private InputStream inputStream;

  public PythonObjectInputStream(InputStream in) throws IOException {
    super(in);
    inputStream = in;
    super.enableResolveObject(true);
  }

  @Override
  protected Object resolveObject(Object obj) throws IOException {
    System.out.println("*** resolve " + obj + " " + obj.getClass());
    return super.resolveObject(obj);
  }
}
