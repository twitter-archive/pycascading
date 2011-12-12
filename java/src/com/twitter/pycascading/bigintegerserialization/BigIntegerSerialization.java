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

import cascading.tuple.Comparison;

import java.math.BigInteger;
import java.util.Comparator;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * Hadoop Serialization class for Java BigIntegers.
 * 
 * @author Gabor Szabo
 */
public class BigIntegerSerialization implements Serialization<BigInteger>, Comparison<BigInteger> {

  public boolean accept(Class<?> c) {
    boolean ret = BigInteger.class.isAssignableFrom(c);
    return ret;
  }

  public Deserializer<BigInteger> getDeserializer(Class<BigInteger> c) {
    return new BigIntegerDeserializer(c);
  }

  public Serializer<BigInteger> getSerializer(Class<BigInteger> c) {
    return new BigIntegerSerializer();
  }

  public Comparator<BigInteger> getComparator(Class<BigInteger> type) {
    return new BigIntegerComparator(type);
  }
}
