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
import java.io.InputStream;
import java.math.BigInteger;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;

/**
 * Hadoop Deserializer for Java BigIntegers.
 * 
 * @author Gabor Szabo
 */
public class BigIntegerDeserializer implements Deserializer<BigInteger> {
  private DataInputStream in;

  public BigIntegerDeserializer(Class<BigInteger> c) {
  }

  public void open(InputStream inStream) throws IOException {
    in = new DataInputStream(inStream);
  }

  public BigInteger deserialize(BigInteger i) throws IOException {
    return BigInteger.valueOf(WritableUtils.readVLong(in));
  }

  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}
