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

import java.io.ObjectInputStream;
import java.io.Serializable;

import org.python.core.Py;
import org.python.core.PyObject;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;

/**
 * Wrapper for a Cascading Filter that calls a Python function.
 * 
 * @author Gabor Szabo
 */
@SuppressWarnings("rawtypes")
public class CascadingFilterWrapper extends CascadingBaseOperationWrapper implements Filter,
        Serializable {
  private static final long serialVersionUID = -8825679328970045134L;

  public CascadingFilterWrapper() {
    super();
  }

  public CascadingFilterWrapper(Fields fieldDeclaration) {
    // If we set it to anything other than Fields.ALL, Cascading complains
    super(Fields.ALL);
  }

  public CascadingFilterWrapper(int numArgs) {
    super(numArgs);
  }

  public CascadingFilterWrapper(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }
  
  public int getNumParameters() {
    return 1;
  }
  
  private void readObject(ObjectInputStream stream) {
    setupArgs();
  }

  @Override
  public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
    Object tuple = convertInput(filterCall.getArguments());
    callArgs[0] = Py.java2py(tuple);
    PyObject ret = callFunction();
    return !Py.py2boolean(ret);
  }
}
