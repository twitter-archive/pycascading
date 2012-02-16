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

import java.io.Serializable;
import java.util.Comparator;

import org.python.core.PyObject;

import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.BufferedInputStream;

/**
 * Cascading in-stream comparator for Python objects.
 * 
 * @author Gabor Szabo
 */
public class PythonComparator implements StreamComparator<BufferedInputStream>,
        Comparator<PyObject>, Serializable {
  private static final long serialVersionUID = 3846289449409826723L;

  public PythonComparator(Class<PyObject> type) {
  }

  public int compare(BufferedInputStream lhsStream, BufferedInputStream rhsStream) {
    throw new RuntimeException("Comparing Jython objects is not allowed");
  }

  public int compare(PyObject o1, PyObject o2) {
    throw new RuntimeException("Comparing Jython objects is not allowed");
  }
}
