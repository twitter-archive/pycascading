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
/*
 * This is a class the helps in serializing Jython functions. It seems that Jython
 * functions cannot be serialized, because on the remote end a Jython interpreter
 * has to also be invoked that can interpret the function.
 * 
 * Thus when deserializing, we need to start a Jython interpreter and read the
 * source file where the function was defined in the first place. This also means
 * that we cannot use lambda functions as these cannot be referred to by name.
 * Referring to functions by name is important as it's the function's name and
 * source file that is sent through when serializing.
 * 
 * It only works with Jython >= 2.5.2 because of a previous bug with serializing
 * PyCode (http://bugs.jython.org/issue1601)
 * Still I need to use a custom class loader, because there's a field in PyCode
 * whose class is called "org.python.pycode._pyx0" but such a class does not exist.
 * 
 * When invoking a function, the globals are not restored for that function. Thus
 * for instance imports of Tuples etc. need to be done within the function. I tried
 * to serialize the globals together with func_code, but org.python.core.packagecache.SysPackageManager
 * in Jython is not serializable, and it is apparently appears in the globals. Tried
 * to recompile Jython from sources, but there're too many external libraries missing.
 *
 * Unortunately Cascading serializes Function objects, but Jython cannot
 * serialize PyFunctions due to bugs. But Jython 2.5.2 can serialize
 * func_codes, so we work it around with that and saving the globals
 * separately in a static variable.
 *
 */

package com.twitter.pycascading;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.python.core.PyDictionary;
import org.python.core.PyFunction;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;

/**
 * Class that is primarily responsible for serializing and deserializing a
 * Jython function. It does this by storing the name of the function and
 * reloading the interpreter and source where the function was defined when it
 * becomes necessary to deserialize.
 * 
 * @author Gabor Szabo
 */
@SuppressWarnings("deprecation")
public class PythonFunctionWrapper implements Serializable {
  private static final long serialVersionUID = 4944819638591252128L;

  public static enum RunningMode {
    LOCAL, HADOOP
  }

  private PyObject pythonFunction;
  String funcSource;
  private PyString funcName, sourceFile;
  private RunningMode runningMode;

  private PythonInterpreter interpreter;

  /**
   * This constructor is necessary for the deserialization.
   */
  public PythonFunctionWrapper() {
  }

  public PythonFunctionWrapper(PyFunction function, String funcSource) {
    this.funcName = function.getFuncName();
    this.funcSource = funcSource;
    pythonFunction = function;
    sourceFile = (PyString) function.func_code.__getattr__("co_filename");
  }

  private void writeObject(ObjectOutputStream stream) throws IOException {
    System.out.println("******* calling writefunc " + funcName);
    stream.writeObject(funcName);
    stream.writeObject(funcSource);
    stream.writeObject(sourceFile);
    stream.writeObject(runningMode);
  }

  private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    funcName = (PyString) stream.readObject();
    funcSource = (String) stream.readObject();
    sourceFile = (PyString) stream.readObject();
    runningMode = (RunningMode) stream.readObject();
    System.out.println("******* calling readfunc " + funcName);
  }

  public void prepare(JobConf conf, PythonEnvironment pythonEnvironment) {
    System.out.println("******* calling prepare " + funcName);
    String pycascadingDir = null;
    String sourceDir = null;
    String[] modulePaths = null;
    if (runningMode == RunningMode.HADOOP) {
      try {
        Path[] archives = DistributedCache.getLocalCacheArchives(conf);
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
    getPythonInterpreter();
    interpreter.execfile(pycascadingDir + "python/pycascading/init_module.py");
    interpreter.set("module_name", "m");
    interpreter.set("file_name", sourceDir + sourceFile);
    interpreter.set("module_paths", modulePaths);
    interpreter.set("map_input_file", conf.get("map.input.file"));
    interpreter.set("jobconf", conf);
    interpreter.eval("setup_paths(module_paths)");
    // PyObject module = (PyObject) interpreter
    // .eval("load_source(module_name, file_name, module_paths)");
    // We need to do this so that nested functions can also be used
    interpreter.execfile(sourceDir + sourceFile);
    interpreter.exec(funcSource);
    // pythonFunction = module.__getattr__(funcName);
    pythonFunction = interpreter.get(funcName.asString());
    System.out.println("we got: " + pythonFunction.getClass());
    if (!PyFunction.class.isInstance(pythonFunction)) {
      // function is assumed to be decorated, resulting in a DecoratedFunction.
      // The function was decorated so we need to get the original back
      // Only for performance reasons. It's just as good to comment this
      // out, as a DecoratedFunction is callable anyway.
      // If we were to decorate the functions with other decorators as
      // well, we certainly cannot use this.
      try {
        pythonFunction = ((PyDictionary) (pythonFunction.__getattr__(new PyString("decorators"))))
                .get(new PyString("function"));
      } catch (Exception e) {
        throw new RuntimeException(
                "Expected a Python function or a decorated function. This shouldn't happen.");
      }
    }
  }

  /**
   * Start a new Jython interpreter if it's not started yet.
   * 
   * @return the interpreter instance
   */
  public PythonInterpreter getPythonInterpreter() {
    if (interpreter == null)
      interpreter = new PythonInterpreter();
    return interpreter;
  }

  /**
   * Call the Python function wrapped by this object.
   * 
   * @param args
   *          the arguments to the Python function
   * @param contextKwArgsNames
   *          the names of the keywords that were (possibly) used when building
   *          the flow using this function
   * @return the returned value of the Python function
   */
  public PyObject callFunction(PyObject[] args, String[] contextKwArgsNames) {
    // Got the idea from:
    // http://osdir.com/ml/lang.jython.devel/2006-05/msg00022.html
    if (contextKwArgsNames == null)
      return pythonFunction.__call__(args);
    else
      return pythonFunction.__call__(args, contextKwArgsNames);
  }

  public PyObject getPythonFunction() {
    return pythonFunction;
  }

  /**
   * Sets the running mode to "local" or "hadoop".
   * 
   * @param runningMode
   *          "local" or "hadoop", indicating where the sources should be
   *          reloaded from (Hadoop explodes the jar into a temporary folder)
   */
  public void setRunningMode(RunningMode runningMode) {
    this.runningMode = runningMode;
  }

  /**
   * Get whether the sources should be reloaded from the local file system or
   * from the folder where the Hadoop jar is extracted to.
   * 
   * @return "local" or "hadoop"
   */
  public RunningMode getRunningMode() {
    return runningMode;
  }
}
