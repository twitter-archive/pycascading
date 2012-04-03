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

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * Simple Cascading function that keeps the specified fields only in the tuple
 * stream.
 * 
 * @author Gabor Szabo
 */
public class SelectFields extends BaseOperation implements Function, Serializable {
  private static final long serialVersionUID = -6859909716154224842L;

  private Fields filteredFileds;

  public SelectFields(Fields filteredFileds) {
    super(filteredFileds);
    this.filteredFileds = filteredFileds;
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    super.prepare(flowProcess, operationCall);
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    TupleEntry inputTuple = functionCall.getArguments();
    TupleEntryCollector outputCollector = functionCall.getOutputCollector();
    Tuple outputTuple = new Tuple();

    for (Comparable field : filteredFileds) {
      // We cannot use inputTuple.get(...) here, as that tries to convert
      // the field value to a Comparable. In case we have a complex Python
      // type as a field, that won't work.
      outputTuple.add(inputTuple.getObject(field));
    }
    outputCollector.add(outputTuple);
  }
}
