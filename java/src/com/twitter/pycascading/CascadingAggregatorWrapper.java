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
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * Wrapper for a Cascading Aggregator that calls a Python function.
 * TODO: we don't really need this, as Buffers are just as good as Aggregators
 * 
 * @author Gabor Szabo
 */
@SuppressWarnings("rawtypes")
public class CascadingAggregatorWrapper extends CascadingRecordProducerWrapper implements
        Aggregator, Serializable {
  private static final long serialVersionUID = -5110929817978998473L;

  public CascadingAggregatorWrapper() {
    super();
  }

  public CascadingAggregatorWrapper(Fields fieldDeclaration) {
    super(fieldDeclaration);
  }

  public CascadingAggregatorWrapper(int numArgs) {
    super(numArgs);
  }

  public CascadingAggregatorWrapper(int numArgs, Fields fieldDeclaration) {
    super(numArgs, fieldDeclaration);
  }

  @Override
  public void start(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
    // TODO Auto-generated method stub
    System.out.println("Aggregator start called");
  }

  @Override
  public void aggregate(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
    TupleEntry group = aggregatorCall.getGroup();
    TupleEntryCollector outputCollector = aggregatorCall.getOutputCollector();

    System.out.println("Aggregator called with group: " + group);
  }

  @Override
  public void complete(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
    // TODO Auto-generated method stub
    System.out.println("Aggregator complete called");
  }
}
