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

import cascading.tuple.Comparison;

import java.util.Comparator;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.python.core.PyObject;

/**
 * Hadoop Serialization class for Python objects.
 * 
 * @author Gabor Szabo
 */
public class PythonSerialization implements Serialization<PyObject>, Comparison<PyObject> {

  public boolean accept(Class<?> c) {
    boolean ret = PyObject.class.isAssignableFrom(c);
    return ret;
  }

  public Deserializer<PyObject> getDeserializer(Class<PyObject> c) {
    return new PythonDeserializer(c);
  }

  public Serializer<PyObject> getSerializer(Class<PyObject> c) {
    return new PythonSerializer();
  }

  public Comparator<PyObject> getComparator(Class<PyObject> type) {
    return new PythonComparator(type);
  }
}
