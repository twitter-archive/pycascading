package com.twitter.pycascading;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cascading.flow.Flow;
import cascading.flow.FlowListener;

public class TemporaryHdfs implements FlowListener {
  private String tmpDir;

  @Override
  public void onStarting(Flow flow) {
  }

  @Override
  public void onStopping(Flow flow) {
    removeTmpDir();
  }

  @Override
  public void onCompleted(Flow flow) {
    removeTmpDir();
  }

  @Override
  public boolean onThrowable(Flow flow, Throwable throwable) {
    removeTmpDir();
    throwable.printStackTrace();
    return false;
  }

  private String getRandomFileName() {
    String name = "";
    Random rnd = new Random();
    for (int i = 0; i < 6; i++) {
      name += (char) ((int) 'a' + rnd.nextInt((int) 'z' - (int) 'a'));
    }
    return name;
  }

  public void createTmpFolder(Configuration conf) throws IOException {
    // Only fs.default.name and hadoop.tmp.dir are defined at the time of the
    // job initialization, we cannot use mapreduce.job.dir, mapred.working.dir,
    // or mapred.job.id
    // Possibly use Hfs.getTempDir later from Cascading.
    // In tmpDir, I cannot put a / in between the two variables, otherwise
    // Hadoop will fail to copy the archive to the temporary folder
    tmpDir = conf.get("fs.default.name") + conf.get("hadoop.tmp.dir");
    tmpDir = tmpDir + "/" + "pycascading-" + getRandomFileName();
    Path path = new Path(tmpDir);
    FileSystem fs = path.getFileSystem(new Configuration());
    fs.mkdirs(path);
  }

  private void removeTmpDir() {
    Path path = new Path(tmpDir);
    try {
      FileSystem fs = path.getFileSystem(new Configuration());
      fs.delete(path, true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String getExtension(String path) {
    int i = path.lastIndexOf('.');
    return (i >= 0 ? path.substring(i, path.length()) : "");
  }

  public String copyFromLocalFile(String source) throws IOException {
    Path src = new Path(source);
    String destName = tmpDir + "/" + getRandomFileName() + getExtension(source);
    Path dest = new Path(destName);
    FileSystem fs = dest.getFileSystem(new Configuration());
    fs.copyFromLocalFile(src, dest);
    return destName;
  }
}
