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
  private ObjectOutputStream out;

  public void open(OutputStream outStream) throws IOException {
    out = new ObjectOutputStream(outStream);
  }

  public void serialize(PyObject i) throws IOException {
    out.writeObject(i);
  }

  public void close() throws IOException {
    if (out != null) {
      out.close();
    }
  }
}
