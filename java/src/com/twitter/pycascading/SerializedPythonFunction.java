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
 */

package com.twitter.pycascading;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.python.core.PyFunction;
import org.python.core.PyObject;
import org.python.core.PyTuple;

/**
 * Class that is primarily responsible for serializing and deserializing a
 * Jython function. It does this by storing the name of the function and
 * reloading the interpreter and source where the function was defined when it
 * becomes necessary to deserialize.
 * 
 * @author Gabor Szabo
 */
public class SerializedPythonFunction implements Serializable {
  private static final long serialVersionUID = 4944819638591252128L;

  private PyObject pythonFunction;
  private PyTuple serializedFunction;

  /**
   * This constructor is necessary for the deserialization.
   */
  public SerializedPythonFunction() {
  }

  public SerializedPythonFunction(PyFunction function, PyTuple serializedReturn) {
    serializedFunction = serializedReturn;
    pythonFunction = function;
  }

  private void writeObject(ObjectOutputStream stream) throws IOException {
    stream.writeObject(serializedFunction);
  }

  private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    serializedFunction = (PyTuple) stream.readObject();
  }

  public PyObject getPythonFunction() {
    return pythonFunction;
  }

  public PyTuple getSerializedFunction() {
    return serializedFunction;
  }
}
