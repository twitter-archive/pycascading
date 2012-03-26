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
import java.io.Serializable;
import java.util.Iterator;

import org.python.core.Py;

import cascading.flow.FlowProcess;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * Wrapper for a Cascading Buffer that calls a Python function.
 * 
 * @author Gabor Szabo
 */
@SuppressWarnings("rawtypes")
public class CascadingBufferWrapper extends CascadingRecordProducerWrapper implements Buffer,
        Serializable {
  private static final long serialVersionUID = -3512295576396796360L;

  public CascadingBufferWrapper() {
    super();
  }

  public CascadingBufferWrapper(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public CascadingBufferWrapper(int numArgs) {
    super(numArgs);
  }

  public CascadingBufferWrapper(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }

  private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    setupArgs();
  }

  public int getNumParameters() {
    return super.getNumParameters() + 1;
  }

  @Override
  public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
    // TODO: if the Python buffer expects Python dicts or lists, then we need to
    // convert the Iterator
    @SuppressWarnings("unchecked")
    Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();

    // This gets called even when there are not tuples for grouping keys after
    // a CoGroup (see Buffer javadoc). So we need to check if there are any
    // valid tuples returned in the group.
    if (arguments.hasNext()) {
      TupleEntry group = bufferCall.getGroup();
      TupleEntryCollector outputCollector = bufferCall.getOutputCollector();

      callArgs[0] = Py.java2py(group);
      callArgs[1] = Py.java2py(arguments);
      if (outputMethod == OutputMethod.COLLECTS) {
        callArgs[2] = Py.java2py(outputCollector);
        if (flowProcessPassIn == FlowProcessPassIn.YES)
          callArgs[3] = Py.java2py(flowProcess);
        callFunction();
      } else {
        if (flowProcessPassIn == FlowProcessPassIn.YES)
          callArgs[2] = Py.java2py(flowProcess);
        Object ret = callFunction();
        collectOutput(outputCollector, ret);
      }
    }
  }
}
