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

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;

/**
 * Wrapper for a Cascading Function that calls a Python function.
 * 
 * @author Gabor Szabo
 */
@SuppressWarnings("rawtypes")
public class CascadingFunctionWrapper extends CascadingRecordProducerWrapper implements Function,
        Serializable {
  private static final long serialVersionUID = -3512295576396796360L;

  public CascadingFunctionWrapper() {
    super();
  }

  public CascadingFunctionWrapper(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public CascadingFunctionWrapper(int numArgs) {
    super(numArgs);
  }

  public CascadingFunctionWrapper(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }

  /**
   * We need to call setupArgs() from here, otherwise CascadingFunctionWrapper
   * is not initialized yet if we call it from CascadingBaseOperationWrapper.
   */
  private void readObject(ObjectInputStream stream) {
    setupArgs();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    // TODO Auto-generated method stub
    super.prepare(flowProcess, operationCall);
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    Object inputTuple = convertInput(functionCall.getArguments());
    TupleEntryCollector outputCollector = functionCall.getOutputCollector();
    
    callArgs[0] = Py.java2py(inputTuple);
    if (outputMethod == OutputMethod.COLLECTS) {
      // The Python function collects the output tuples itself into the output
      // collector
      callArgs[1] = Py.java2py(outputCollector);
      if (flowProcessPassIn == FlowProcessPassIn.YES)
        callArgs[2] = Py.java2py(flowProcess);
      callFunction();
    } else {
      // The Python function yields or returns records
      if (flowProcessPassIn == FlowProcessPassIn.YES)
        callArgs[1] = Py.java2py(flowProcess);
      Object ret = callFunction();
      collectOutput(outputCollector, ret);
    }
  }
}
