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
package com.twitter.pycascading;

import java.io.Serializable;

import org.python.core.PyGenerator;
import org.python.core.PyNone;
import org.python.core.PyObject;
import org.python.core.PySequenceList;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * This class is the parent class for Cascading Functions and Buffers. It
 * essetially converts records coming from the Python function to tuples.
 * 
 * @author Gabor Szabo
 */
public class CascadingRecordProducerWrapper extends CascadingBaseOperationWrapper implements
        Serializable {
  private static final long serialVersionUID = -1198203231681047370L;

  // This is how the Python function returns the output tuples. It can add them
  // to the output collector right away, provide a generator to yield one or
  // more records, or return one record only. YIELDS_OR_RETURNS means that
  // PyCascading should determine automatically if it's a generator or a normal
  // function.
  public enum OutputMethod {
    COLLECTS, YIELDS, RETURNS, YIELDS_OR_RETURNS
  }

  // This is what the Python function returns: a Python list or a Cascading
  // tuple, or PyCascading can also figure it out automatically from the first
  // record returned.
  //
  // AUTO means that the type of the very first object returned from the
  // Python @map determines what type we are going to use.
  public enum OutputType {
    AUTO, PYTHON_LIST, TUPLE, TUPLEENTRY
  }

  protected OutputMethod outputMethod;
  protected OutputType outputType;

  public CascadingRecordProducerWrapper() {
    super();
  }

  public CascadingRecordProducerWrapper(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public CascadingRecordProducerWrapper(int numArgs) {
    super(numArgs);
  }

  public CascadingRecordProducerWrapper(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }

  public int getNumParameters() {
    return (outputMethod == OutputMethod.COLLECTS ? 2 : 1);
  }

  /**
   * Cast the returned or yielded array to a Tuple, and add it to the output
   * collector.
   * 
   * @param ret
   *          the object (list) returned from the Python function
   * @param outputCollector
   *          the output collector in which we place the Tuple
   * @param simpleCastIfTuple
   *          if we can simply cast ret to a Tuple, or have to call Jython's
   *          casting
   */
  private void castPythonObject(Object ret, TupleEntryCollector outputCollector,
          boolean simpleCastIfTuple) {
    if (outputType == OutputType.AUTO) {
      // We need to determine the type of the record now
      if (PySequenceList.class.isInstance(ret))
        outputType = OutputType.PYTHON_LIST;
      else if (Tuple.class.isInstance(ret))
        outputType = OutputType.TUPLE;
      else if (TupleEntry.class.isInstance(ret))
        outputType = OutputType.TUPLEENTRY;
      else
        throw new RuntimeException(
                "Python function must return a list, Tuple, or TupleEnty. We got: "
                        + ret.getClass());
    }
    if (outputType == OutputType.PYTHON_LIST)
      // Convert the returned Python list to a tuple
      // We can return both a Python (immutable) tuple and a list, so we
      // need to use their common superclass, PySequenceList.
      try {
        outputCollector.add(new Tuple(((PySequenceList) ret).toArray()));
      } catch (ClassCastException e) {
        throw new RuntimeException(
                "Python function or generator must return a Python list, we got " + ret.getClass()
                        + " instead");
      }
    else if (outputType == OutputType.TUPLE) {
      try {
        // For some reason yield doesn't wrap the object in a Jython
        // container, but return does
        if (simpleCastIfTuple)
          outputCollector.add((Tuple) ret);
        else
          outputCollector.add((Tuple) ((PyObject) ret).__tojava__(Tuple.class));
      } catch (ClassCastException e) {
        throw new RuntimeException(
                "Python function or generator must return a Cascading Tuple, we got "
                        + ret.getClass() + " instead");
      }
    } else {
      try {
        outputCollector.add((TupleEntry) ((PyObject) ret).__tojava__(TupleEntry.class));
      } catch (ClassCastException e) {
        throw new RuntimeException(
                "Python function or generator must return a Cascading TupleEntry, we got "
                        + ret.getClass() + " instead");
      }
    }
  }

  protected void collectOutput(TupleEntryCollector outputCollector, Object ret) {
    if (ret == null)
      return;
    if (outputMethod == OutputMethod.YIELDS_OR_RETURNS) {
      // Determine automatically whether the function yields or returns
      outputMethod = (PyGenerator.class.isInstance(ret) ? OutputMethod.YIELDS
              : OutputMethod.RETURNS);
    }
    if (outputMethod == OutputMethod.RETURNS) {
      // We're simply returning records
      // We can return None to produce no output
      if (PyNone.class.isInstance(ret))
        return;
      castPythonObject(ret, outputCollector, false);
    } else {
      // We have a Python generator that yields records
      for (Object record : (PyGenerator) ret) {
        if (record != null) {
          castPythonObject(record, outputCollector, true);
        }
      }
    }
  }

  public void setOutputMethod(OutputMethod outputMethod) {
    this.outputMethod = outputMethod;
  }

  public void setOutputType(OutputType outputType) {
    this.outputType = outputType;
  }
}
