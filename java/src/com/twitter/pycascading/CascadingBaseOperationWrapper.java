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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.python.core.Py;
import org.python.core.PyDictionary;
import org.python.core.PyFunction;
import org.python.core.PyIterator;
import org.python.core.PyList;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PyTuple;
import org.python.util.PythonInterpreter;

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
@SuppressWarnings({ "rawtypes", "deprecation" })
public class CascadingBaseOperationWrapper extends BaseOperation implements Serializable {
  private static final long serialVersionUID = -535185466322890691L;

  // This defines whether the input tuples should be converted to Python lists
  // or dicts before passing them to the Python function
  public enum ConvertInputTuples {
    NONE, PYTHON_LIST, PYTHON_DICT
  }

  private PyObject function;
  private ConvertInputTuples convertInputTuples;
  private PyTuple contextArgs = null;
  protected PyDictionary contextKwArgs = null;

  private PyFunction writeObjectCallBack;
  private byte[] serializedFunction;

  // These are some variables to optimize the frequent UDF calls
  protected PyObject[] callArgs = null;
  private String[] contextKwArgsNames = null;

  /**
   * Class to convert elements in an iterator to corresponding Jython objects.
   * 
   * @author Gabor Szabo
   * 
   * @param <I>
   *          the type of the items
   */
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

  private PythonInterpreter setupInterpreter(JobConf jobConf, FlowProcess flowProcess) {
    String pycascadingDir = null;
    String sourceDir = null;
    String[] modulePaths = null;
    if ("hadoop".equals(jobConf.get("pycascading.running_mode"))) {
      try {
        Path[] archives = DistributedCache.getLocalCacheArchives(jobConf);
        pycascadingDir = archives[0].toString() + "/";
        sourceDir = archives[1].toString() + "/";
        modulePaths = new String[archives.length];
        int i = 0;
        for (Path archive : archives) {
          modulePaths[i++] = archive.toString();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      pycascadingDir = System.getProperty("pycascading.root") + "/";
      sourceDir = "";
      modulePaths = new String[] { pycascadingDir, sourceDir };
    }
    PythonInterpreter interpreter = Main.getInterpreter();
    interpreter.execfile(pycascadingDir + "python/pycascading/init_module.py");
    interpreter.set("module_paths", modulePaths);
    interpreter.eval("setup_paths(module_paths)");

    // We set the Python variable "map_input_file" to the path to the mapper
    // input file
    interpreter.set("map_input_file", jobConf.get("map.input.file"));

    // We set the Python variable "jobconf" to the MR jobconf
    interpreter.set("jobconf", jobConf);

    // The flowProcess passed to the Operation is passed on to the Python
    // function in the variable flow_process
    interpreter.set("flow_process", flowProcess);

    // We need to run the main file first so that imports etc. are defined,
    // and nested functions can also be used
    interpreter.execfile(sourceDir + (String) jobConf.get("pycascading.main_file"));
    return interpreter;
  }

  // We need to delay the deserialization of the Python functions up to this
  // point, since the sources are in the distributed cache, whose location is in
  // the jobconf, and we get access to the jobconf only at this point for the
  // first time.
  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    JobConf jobConf = ((HadoopFlowProcess) flowProcess).getJobConf();
    PythonInterpreter interpreter = setupInterpreter(jobConf, flowProcess);

    ByteArrayInputStream baos = new ByteArrayInputStream(serializedFunction);
    try {
      PythonObjectInputStream pythonStream = new PythonObjectInputStream(baos, interpreter);

      function = (PyObject) pythonStream.readObject();
      convertInputTuples = (ConvertInputTuples) pythonStream.readObject();
      if ((Boolean) pythonStream.readObject())
        contextArgs = (PyTuple) pythonStream.readObject();
      if ((Boolean) pythonStream.readObject())
        contextKwArgs = (PyDictionary) pythonStream.readObject();
      baos.close();
    } catch (Exception e) {
      // If there are any kind of exceptions (ClassNotFoundException or
      // IOException), we don't want to continue.
      throw new RuntimeException(e);
    }
    serializedFunction = null;
    if (!PyFunction.class.isInstance(function)) {
      // function is assumed to be decorated, resulting in a
      // DecoratedFunction, so we can get the original function back.
      //
      // Only for performance reasons. It's just as good to comment this
      // out, as a DecoratedFunction is callable anyway.
      // If we were to decorate the functions with other decorators as
      // well, we certainly cannot use this.
      try {
        function = (PyFunction) ((PyDictionary) (function.__getattr__(new PyString("decorators"))))
                .get(new PyString("function"));
      } catch (Exception e) {
        throw new RuntimeException(
                "Expected a Python function or a decorated function. This shouldn't happen.");
      }
    }
    setupArgs();
  }

  private void writeObject(ObjectOutputStream stream) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PythonObjectOutputStream pythonStream = new PythonObjectOutputStream(baos, writeObjectCallBack);
    pythonStream.writeObject(function);
    pythonStream.writeObject(convertInputTuples);
    pythonStream.writeObject(new Boolean(contextArgs != null));
    if (contextArgs != null) {
      pythonStream.writeObject(contextArgs);
    }
    pythonStream.writeObject(new Boolean(contextKwArgs != null));
    if (contextKwArgs != null)
      pythonStream.writeObject(contextKwArgs);
    pythonStream.close();

    stream.writeObject(baos.toByteArray());
  }

  private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException,
          URISyntaxException {
    // TODO: we need to start up the interpreter and for all the imports, as
    // the parameters may use other imports, like datetime. Or how else can
    // we do this better?
    serializedFunction = (byte[]) stream.readObject();
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
   * Sets up the local variables that were not serialized for optimizations.
   */
  protected void setupArgs() {
    int numArgs = getNumParameters();
    callArgs = new PyObject[numArgs + (contextArgs == null ? 0 : contextArgs.size())
            + (contextKwArgs == null ? 0 : contextKwArgs.size())];
    int i = numArgs;
    if (contextArgs != null) {
      PyObject[] args = contextArgs.getArray();
      for (PyObject arg : args) {
        callArgs[i] = arg;
        i++;
      }
    }
    if (contextKwArgs != null) {
      PyIterator values = (PyIterator) contextKwArgs.itervalues();
      PyObject value = values.__iternext__();
      while (value != null) {
        callArgs[i] = value;
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
    if (contextKwArgsNames == null)
      return function.__call__(callArgs);
    else
      return function.__call__(callArgs, contextKwArgsNames);
  }

  /**
   * Setter for the Python function object.
   * 
   * @param function
   *          the Python function
   */
  public void setFunction(PyFunction function) {
    this.function = function;
  }

  /**
   * Setter for the input tuple conversion type.
   * 
   * @param convertInputTuples
   *          whether to do any conversion on input tuples, and the type of the
   *          converted tuple (none/list/dict)
   */
  public void setConvertInputTuples(ConvertInputTuples convertInputTuples) {
    this.convertInputTuples = convertInputTuples;
  }

  /**
   * Setter for the constant unnamed arguments that are passed in for the UDF
   * aside from the tuples.
   * 
   * @param args
   *          the additional unnamed arguments
   */
  public void setContextArgs(PyTuple args) {
    contextArgs = args;
    setupArgs();
  }

  /**
   * Setter for the constant named arguments that are passed in for the UDF
   * aside from the tuples.
   * 
   * @param args
   *          the additional unnamed arguments
   */
  public void setContextKwArgs(PyDictionary kwargs) {
    contextKwArgs = kwargs;
    setupArgs();
  }

  /**
   * The Python callback function to call to get the source of a PyFunction. We
   * better do it in Python using the inspect module, than hack it around in
   * Java.
   * 
   * @param callBack
   *          the PyFunction that is called to get the source of a Python
   *          function
   */
  public void setWriteObjectCallBack(PyFunction callBack) {
    this.writeObjectCallBack = callBack;
  }
}
