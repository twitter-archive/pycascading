package com.twitter.pycascading;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cascading.flow.Flow;
import cascading.flow.FlowListener;

public class TemporaryHdfs implements FlowListener {
  private boolean tmpDirCreated = false;
  private String tmpDir;

  @Override
  public void onStarting(Flow flow) {
    System.out.println("************* Flow starting " + flow);
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

  /**
   * Create a temporary folder on HDFS. The folder will be deleted after
   * execution or on an exception.
   * 
   * @param conf
   *          the jobconf
   * @throws IOException
   */
  String createTmpFolder(Configuration conf) throws IOException {
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
    tmpDirCreated = true;
    return tmpDir;
  }

  /**
   * Removes the temporary folder we created.
   */
  private void removeTmpDir() {
    if (tmpDirCreated) {
      Path path = new Path(tmpDir);
      try {
        FileSystem fs = path.getFileSystem(new Configuration());
        fs.delete(path, true);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private String getExtension(String path) {
    int i = path.lastIndexOf('.');
    return (i >= 0 ? path.substring(i, path.length()) : "");
  }

  /**
   * Copies a local file to HDFS, which is used as the distributed cache. The
   * distribute cache basically just takes this HDFS folder, and copies its
   * contents to the local disks for the mappers/reducers. Also, if the file is
   * a compressed archive, it will extract it locally. We generate a random file
   * name for the destination, but keep the extension so that zip and tgz
   * archives are recognized.
   * 
   * @param source
   *          the path to the local file to be distributed
   * @return the path to the HDFS file
   * @throws IOException
   *           if the copy was unsuccessful
   */
  public String copyFromLocalFileToHDFS(String source) throws IOException {
    Path src = new Path(source);
    String destName = tmpDir + "/" + getRandomFileName() + getExtension(source);
    Path dest = new Path(destName);
    FileSystem fs = dest.getFileSystem(new Configuration());
    fs.copyFromLocalFile(src, dest);
    return destName;
  }
}
