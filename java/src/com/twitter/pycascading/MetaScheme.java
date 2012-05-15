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
import java.io.ObjectOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * A Cascading Scheme that stores header information for an output dataset. It
 * records all formatting information so that later on the tuple field names and
 * types can be reloaded without having to specify them explicitly.
 * 
 * It also stores the original scheme object so that at load time we don't have
 * to worry about that either.
 * 
 * @author Gabor Szabo
 */
public class MetaScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
  private static final long serialVersionUID = 8194175541999063797L;

  private static final String schemeFileName = ".pycascading_scheme";
  private static final String headerFileName = ".pycascading_header";
  private static final String typeFileName = ".pycascading_types";

  private Scheme scheme;
  private String outputPath;
  private boolean firstLine = true;
  private boolean typeFileToWrite = true;

  /**
   * Call this to get the original Cascading scheme that the data was written
   * in.
   * 
   * @param inputPath
   *          The path to where the scheme information was stored (normally the
   *          same as the path to the data)
   * @return The Cascading scheme that was used when the data was written.
   * @throws IOException
   */
  public static Scheme getSourceScheme(String inputPath) throws IOException {
    Path path = new Path(inputPath + "/" + schemeFileName);
    FileSystem fs = path.getFileSystem(new Configuration());
    try {
      FSDataInputStream file = fs.open(path);
      ObjectInputStream ois = new ObjectInputStream(file);
      Scheme scheme = (Scheme) ois.readObject();
      Fields fields = (Fields) ois.readObject();
      scheme.setSourceFields(fields);
      ois.close();
      file.close();
      return scheme;
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not read PyCascading file header: " + inputPath + "/"
              + schemeFileName);
    }
  }

  /**
   * Returns the scheme that will store field information and the scheme in
   * outputPath. Additionally, a file called .pycascading_header will be
   * generated, which stores the names of the fields in a TAB-delimited format.
   * 
   * @param scheme
   *          The Cascading scheme to be used to store the data
   * @param outputPath
   *          Path were the metainformation about the scheme and field names
   *          should be stored
   * @return A scheme that can be used to sink the data into
   * @throws IOException
   */
  public static Scheme getSinkScheme(Scheme scheme, String outputPath) throws IOException {
    return new MetaScheme(scheme, outputPath);
  }

  protected MetaScheme(Scheme scheme, String outputPath) throws IOException {
    this.scheme = scheme;
    this.outputPath = outputPath;
  }

  @Override
  public void sourceConfInit(FlowProcess<JobConf> process,
          Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    // We're returning the original storage scheme, so this should not be called
    // ever.
  }

  @Override
  public boolean source(FlowProcess<JobConf> flowProcess,
          SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    // This should never be called.
    return false;
  }

  @Override
  public void sinkConfInit(FlowProcess<JobConf> process,
          Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
    scheme.sinkConfInit(process, tap, conf);
  }

  @Override
  public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall)
          throws IOException {
    OutputCollector outputCollector = sinkCall.getOutput();
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
    if (firstLine) {
      Path path = new Path(outputPath + "/" + headerFileName);
      FileSystem fs = path.getFileSystem(new Configuration());
      try {
        // We're trying to create the file by just one of the mappers/reducers,
        // the one that can do it first
        if (fs.createNewFile(path)) {
          FSDataOutputStream stream = fs.create(path, true);
          boolean firstField = true;
          for (Comparable<?> field : tupleEntry.getFields()) {
            if (firstField)
              firstField = false;
            else
              stream.writeBytes("\t");
            stream.writeBytes(field.toString());
          }
          stream.writeBytes("\n");
          stream.close();
        }
      } catch (IOException e) {
      }

      path = new Path(outputPath + "/" + schemeFileName);
      fs = path.getFileSystem(new Configuration());
      try {
        if (fs.createNewFile(path)) {
          FSDataOutputStream stream = fs.create(path, true);
          ObjectOutputStream ostream = new ObjectOutputStream(stream);
          ostream.writeObject(scheme);
          ostream.writeObject(tupleEntry.getFields());
          ostream.close();
          stream.close();
        }
      } catch (IOException e) {
      }
      firstLine = false;
    }

    if (typeFileToWrite) {
      Path path = new Path(outputPath + "/" + typeFileName);
      FileSystem fs = path.getFileSystem(new Configuration());
      try {
        if (fs.createNewFile(path)) {
          FSDataOutputStream stream = fs.create(path, true);
          for (int i = 0; i < tupleEntry.size(); i++) {
            Comparable fieldName = null;
            if (tupleEntry.getFields().size() < tupleEntry.size()) {
              // We don't have names for the fields
              fieldName = "";
            } else {
              fieldName = tupleEntry.getFields().get(i) + "\t";
            }
            Object object = tupleEntry.getObject(i);
            Class<?> objectClass = (object == null ? Object.class : object.getClass());
            stream.writeBytes(fieldName + objectClass.getName() + "\n");
          }
          stream.close();
        }
      } catch (IOException e) {
      }
      typeFileToWrite = false;
    }
    scheme.sink(flowProcess, sinkCall);
  }

  // We need to delegate all sink-related calls to
  @Override
  public int getNumSinkParts() {
    return scheme.getNumSinkParts();
  }

  @Override
  public Fields getSinkFields() {
    return scheme.getSinkFields();
  }

  @Override
  public Fields getSourceFields() {
    return scheme.getSourceFields();
  }

  @Override
  public String getTrace() {
    return scheme.getTrace();
  }

  @Override
  public boolean isSymmetrical() {
    return scheme.isSymmetrical();
  }

  @Override
  public void presentSinkFields(FlowProcess<JobConf> flowProcess, Tap tap, Fields fields) {
    scheme.presentSinkFields(flowProcess, tap, fields);
  }

  @Override
  public void presentSourceFields(FlowProcess<JobConf> flowProcess, Tap tap, Fields fields) {
    scheme.presentSourceFields(flowProcess, tap, fields);
  }

  @Override
  public Fields retrieveSinkFields(FlowProcess<JobConf> flowProcess, Tap tap) {
    return scheme.retrieveSinkFields(flowProcess, tap);
  }

  @Override
  public void setNumSinkParts(int numSinkParts) {
    scheme.setNumSinkParts(numSinkParts);
  }

  @Override
  public void setSinkFields(Fields sinkFields) {
    scheme.setSinkFields(sinkFields);
  }

  @Override
  public void sinkCleanup(FlowProcess<JobConf> flowProcess,
          SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
    scheme.sinkCleanup(flowProcess, sinkCall);
  }

  @Override
  public void sinkPrepare(FlowProcess<JobConf> flowProcess,
          SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
    scheme.sinkPrepare(flowProcess, sinkCall);
  }

  @Override
  public String toString() {
    return scheme.toString();
  }
}
