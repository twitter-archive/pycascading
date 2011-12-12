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
package com.twitter.pycascading.bigintegerserialization;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Comparator;

import org.apache.hadoop.io.WritableUtils;

import cascading.tuple.StreamComparator;
import cascading.tuple.hadoop.BufferedInputStream;

/**
 * Cascading in-stream comparator for Java BigIntegers.
 * 
 * @author Gabor Szabo
 */
public class BigIntegerComparator implements StreamComparator<BufferedInputStream>,
        Comparator<BigInteger>, Serializable {
  private static final long serialVersionUID = 3846289449409826723L;

  public BigIntegerComparator(Class<BigInteger> type) {
  }

  public int compare(BufferedInputStream lhsStream, BufferedInputStream rhsStream) {
    try {
      DataInputStream inLeft = new DataInputStream(lhsStream);
      DataInputStream inRight = new DataInputStream(rhsStream);

      long lhs = WritableUtils.readVLong(inLeft);
      long rhs = WritableUtils.readVLong(inRight);

      if (lhs < rhs)
        return -1;
      else if (lhs > rhs)
        return 1;
      else
        return 0;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public int compare(BigInteger o1, BigInteger o2) {
    return o1.compareTo(o2);
  }
}
