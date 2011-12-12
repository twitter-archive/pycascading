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
  public enum OutputType {
    AUTO, PYTHON_LIST, TUPLE
  }

  // Pass the FlowProcess to the Python function
  public enum FlowProcessPassIn {
    YES, NO
  }

  protected OutputMethod outputMethod;
  protected OutputType outputType;
  protected FlowProcessPassIn flowProcessPassIn;

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
    int n = (outputMethod == OutputMethod.COLLECTS ? 2 : 1);
    if (flowProcessPassIn == FlowProcessPassIn.YES)
      n++;
    return n;
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
      Tuple tuple = null;
      // We can return None to produce no output
      if (PyNone.class.isInstance(ret))
        return;
      if (outputType == OutputType.AUTO)
        outputType = (PySequenceList.class.isInstance(ret) ? OutputType.PYTHON_LIST
                : OutputType.TUPLE);
      if (outputType == OutputType.PYTHON_LIST)
        // Convert the returned Python list to a tuple
        // We can return both a Python tuple and a list, so we need to use
        // their common superclass, PySequenceList
        try {
          tuple = new Tuple(((PySequenceList) ret).toArray());
        } catch (ClassCastException e) {
          throw new RuntimeException("Python function must return a Python list, instead we got "
                  + ret.getClass() + " instead");
        }
      else
        try {
          tuple = (Tuple) ((PyObject) ret).__tojava__(Tuple.class);
        } catch (ClassCastException e) {
          throw new RuntimeException("Python function must return a Cascading tuple, we got "
                  + ret.getClass() + " instead");
        }
      outputCollector.add(tuple);
    } else {
      // We have a Python generator that yields records
      for (Object record : (PyGenerator) ret) {
        if (record != null) {
          Tuple tuple = null;
          if (outputType == OutputType.AUTO)
            // We need to determine the type of the record now
            outputType = (PySequenceList.class.isInstance(record) ? OutputType.PYTHON_LIST
                    : OutputType.TUPLE);
          if (outputType == OutputType.PYTHON_LIST)
            tuple = new Tuple(((PySequenceList) record).toArray());
          else if (outputType == OutputType.TUPLE)
            try {
              // For some reason yield doesn't wrap the object in a Jython
              // container, but
              // return does
              tuple = (Tuple) record;
            } catch (ClassCastException e) {
              throw new RuntimeException("Python generator must yield a Cascading tuple, we got "
                      + record.getClass() + " instead");
            }
          outputCollector.add(tuple);
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

  public void setFlowProcessPassIn(FlowProcessPassIn flowProcessPassIn) {
    this.flowProcessPassIn = flowProcessPassIn;
  }
}
