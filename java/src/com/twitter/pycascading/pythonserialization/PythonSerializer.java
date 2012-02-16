/**
 * Copyright 2011 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.pycascading.pythonserialization;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.serializer.Serializer;
import org.python.core.PyObject;

/**
 * Hadoop Serializer for Python objects.
 * 
 * @author Gabor Szabo
 */
public class PythonSerializer implements Serializer<PyObject> {
  private OutputStream outStream;

  public void open(OutputStream outStream) throws IOException {
    this.outStream = outStream;
  }

  public void serialize(PyObject i) throws IOException {
    // We have to create an ObjectOutputStream here. If we do it in open(...),
    // the following exception will be thrown on the reducers from
    // PythonDeserializer with large jobs:
    // java.io.StreamCorruptedException: invalid stream header: 7371007E
    // TODO: check if a flush wouldn't be enough
    ObjectOutputStream out = new ObjectOutputStream(outStream);
    out.writeObject(i);
    out.close();
  }

  public void close() throws IOException {
    if (outStream != null) {
      outStream.close();
    }
  }
}
