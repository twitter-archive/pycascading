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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.python.core.Py;
import org.python.core.PyDictionary;
import org.python.core.PyIterator;
import org.python.core.PyList;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PyTuple;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Wrapper for a Cascading BaseOperation that prepares the input tuples for a
 * Python function. It can convert between tuples and Python lists and dicts.
 * 
 * @author Gabor Szabo
 */
@SuppressWarnings("rawtypes")
public class CascadingBaseOperationWrapper extends BaseOperation implements Serializable {
  private static final long serialVersionUID = -535185466322890691L;

  // This defines whether the input tuples should be converted to Python lists
  // or dicts before passing them to the Python function
  public enum ConvertInputTuples {
    NONE, PYTHON_LIST, PYTHON_DICT
  }

  private PythonFunctionWrapper function;
  private ConvertInputTuples convertInputTuples;

  private PyTuple contextArgs = null;
  protected PyDictionary contextKwArgs = null;

  protected PyObject[] callArgs = null;
  private String[] contextKwArgsNames = null;

  class ConvertIterable<I> implements Iterator<PyObject> {
    private Iterator<I> iterator;

    public ConvertIterable(Iterator<I> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public PyObject next() {
      return Py.java2py(iterator.next());
    }

    @Override
    public void remove() {
      iterator.remove();
    }
  }

  /**
   * This is necessary for the deserialization.
   */
  public CascadingBaseOperationWrapper() {
    super();
  }

  public CascadingBaseOperationWrapper(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public CascadingBaseOperationWrapper(int numArgs) {
    super(numArgs);
  }

  public CascadingBaseOperationWrapper(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    function.prepare(((HadoopFlowProcess) flowProcess).getJobConf());
  }

  private void writeObject(ObjectOutputStream stream) throws IOException {
    stream.writeObject(function);
    stream.writeObject(convertInputTuples);
    stream.writeObject(new Boolean(contextArgs != null));
    if (contextArgs != null)
      stream.writeObject(contextArgs);
    stream.writeObject(new Boolean(contextKwArgs != null));
    if (contextKwArgs != null)
      stream.writeObject(contextKwArgs);
  }

  private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException,
          URISyntaxException {
    function = (PythonFunctionWrapper) stream.readObject();
    convertInputTuples = (ConvertInputTuples) stream.readObject();
    if ((Boolean) stream.readObject())
      contextArgs = (PyTuple) stream.readObject();
    if ((Boolean) stream.readObject())
      contextKwArgs = (PyDictionary) stream.readObject();
  }

  /**
   * We assume that the Python functions (map and reduce) are always called with
   * the same number of arguments. Override this to return the number of
   * arguments we will be passing in all the time.
   * 
   * @return the number of arguments the wrapper is passing in
   */
  public int getNumParameters() {
    return 0;
  }

  /**
   * We may pass in Python functions as an argument to UDFs. In this case we
   * have to wrap these the same way we wrapped the UDFs, and need to unwrap
   * them at deserialization.
   * 
   * @return the original argument to the UDF before serialization
   */
  private PyObject getOriginalArg(PyObject arg) {
    Object result = arg.__tojava__(PythonFunctionWrapper.class);
    if (result == Py.NoConversion)
      return arg;
    else
      return ((PythonFunctionWrapper) result).getPythonFunction();
  }

  /**
   * Sets up the local variables that were not serialized for optimizations and
   * unwraps function arguments wrapped with PythonFunctionWrapper.
   */
  protected void setupArgs() {
    int numArgs = getNumParameters();
    callArgs = new PyObject[numArgs + (contextArgs == null ? 0 : contextArgs.size())
            + (contextKwArgs == null ? 0 : contextKwArgs.size())];
    int i = numArgs;
    if (contextArgs != null) {
      PyObject[] args = contextArgs.getArray();
      for (PyObject arg : args) {
        callArgs[i] = getOriginalArg(arg);
        i++;
      }
    }
    if (contextKwArgs != null) {
      PyIterator values = (PyIterator) contextKwArgs.itervalues();
      PyObject value = values.__iternext__();
      while (value != null) {
        callArgs[i] = getOriginalArg(value);
        value = values.__iternext__();
        i++;
      }

      contextKwArgsNames = new String[contextKwArgs.size()];
      PyIterator keys = (PyIterator) contextKwArgs.iterkeys();
      PyObject key = keys.__iternext__();
      int j = 0;
      while (key != null) {
        contextKwArgsNames[j] = ((PyString) key).asString();
        key = keys.__iternext__();
        j++;
      }
    }
  }

  @SuppressWarnings("unchecked")
  public Object convertInput(TupleEntry tupleEntry) {
    Object result = null;
    if (convertInputTuples == ConvertInputTuples.NONE) {
      // We don't need to convert the tuples
      result = tupleEntry;
    } else if (convertInputTuples == ConvertInputTuples.PYTHON_LIST) {
      // The user wants a Python list
      result = new PyList(new ConvertIterable<Object>(tupleEntry.getTuple().iterator()));
    } else if (convertInputTuples == ConvertInputTuples.PYTHON_DICT) {
      // The user wants a Python dict
      PyObject[] dictElements = new PyObject[2 * tupleEntry.size()];
      // Here we convert Java objects to Jython objects
      // http://osdir.com/ml/lang.jython.devel/2006-05/msg00022.html
      // If the fields are not named in the tuple, generate keys using
      // their integer index.
      int i = 0;
      Iterator<Object> iter = tupleEntry.getFields().iterator();
      while (i < dictElements.length) {
        dictElements[i] = Py.java2py(iter.hasNext() ? iter.next() : i / 2);
        i += 2;
      }
      i = 1;
      for (Object value : tupleEntry.getTuple()) {
        dictElements[i] = Py.java2py(value);
        i += 2;
      }
      PyDictionary dict = new PyDictionary(dictElements);
      result = dict;
    }
    return result;
  }

  /**
   * This calls the Python function on behalf of the BaseOperation. The callArgs
   * field is protected, so that derived classes may put the function parameters
   * into it.
   * 
   * @return the return value of the Python function
   */
  public PyObject callFunction() {
    return function.callFunction(callArgs, contextKwArgsNames);
  }

  public void setFunction(PythonFunctionWrapper function) {
    this.function = function;
  }

  public void setConvertInputTuples(ConvertInputTuples convertInputTuples) {
    this.convertInputTuples = convertInputTuples;
  }

  public void setContextArgs(PyTuple args) {
    contextArgs = args;
    setupArgs();
  }

  public void setContextKwArgs(PyDictionary kwargs) {
    contextKwArgs = kwargs;
    setupArgs();
  }
}
