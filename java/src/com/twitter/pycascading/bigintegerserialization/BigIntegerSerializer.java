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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * Hadoop Serializer for Java BigIntegers.
 * 
 * @author Gabor Szabo
 */
public class BigIntegerSerializer implements Serializer<BigInteger> {
  private DataOutputStream out;

  public void open(OutputStream outStream) throws IOException {
    out = new DataOutputStream(outStream);
  }

  public void serialize(BigInteger i) throws IOException {
    WritableUtils.writeVLong(out, i.longValue());
  }

  public void close() throws IOException {
    if (out != null) {
      out.close();
    }
  }
}
