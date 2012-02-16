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
import java.io.InputStream;
import java.io.ObjectInputStream;

import org.apache.hadoop.io.serializer.Deserializer;
import org.python.core.PyObject;

/**
 * Hadoop Deserializer for Python objects.
 * 
 * @author Gabor Szabo
 */
public class PythonDeserializer implements Deserializer<PyObject> {
  private InputStream inStream;

  public PythonDeserializer(Class<PyObject> c) {
  }

  public void open(InputStream inStream) throws IOException {
    this.inStream = inStream;
  }

  public PyObject deserialize(PyObject i) throws IOException {
    try {
      ObjectInputStream in = new ObjectInputStream(inStream);
      return (PyObject) in.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException("Jython class not found");
    }
  }

  public void close() throws IOException {
    if (inStream != null) {
      inStream.close();
    }
  }
}
