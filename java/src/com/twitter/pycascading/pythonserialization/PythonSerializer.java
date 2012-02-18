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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.serializer.Serializer;
import org.python.core.PyObject;

/**
 * Hadoop Serializer for Python objects.
 * 
 * This is suboptimal, slow, and produces bloated streams, and should not be
 * used in production. In other words it just demonstrates the use of serialized
 * Python objects.
 * 
 * @author Gabor Szabo
 */
public class PythonSerializer implements Serializer<PyObject> {
  private DataOutputStream outStream;

  public void open(OutputStream outStream) throws IOException {
    if (outStream instanceof DataOutputStream)
      this.outStream = (DataOutputStream) outStream;
    else
      this.outStream = new DataOutputStream(outStream);
  }

  public void serialize(PyObject i) throws IOException {
    // We have to create an ObjectOutputStream here. If we do it in open(...),
    // the following exception will be thrown on the reducers from
    // PythonDeserializer with large jobs:
    // java.io.StreamCorruptedException: invalid stream header: 7371007E
    // TODO: check if a flush wouldn't be enough
    ObjectOutputStream out = new ObjectOutputStream(outStream);
    out.writeObject(i);
    // We need to flush the stream, otherwise we get corrupted object stream
    // header exceptions as above.
    // Also do not use close(), as that would close result in
    // java.io.IOException: write beyond end of stream exceptions on spilled
    // cogroups.
    out.flush();
  }

  public void close() throws IOException {
    if (outStream != null) {
      outStream.close();
      outStream = null;
    }
  }
}
