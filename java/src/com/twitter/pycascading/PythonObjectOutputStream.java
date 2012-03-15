package com.twitter.pycascading;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class PythonObjectOutputStream extends ObjectOutputStream {

  private OutputStream outStream;

  public PythonObjectOutputStream(OutputStream out) throws IOException {
    super(out);
    outStream = out;
  }

}
