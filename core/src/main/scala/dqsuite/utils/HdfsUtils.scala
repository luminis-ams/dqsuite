package dqsuite.utils

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession

private[dqsuite] object HdfsUtils {
  /* Make sure we write to the correct filesystem, as EMR clusters also have an internal HDFS */
  def asQualifiedPath(session: SparkSession, path: String): (FileSystem, Path) = {
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(session.sparkContext.hadoopConfiguration)
    val qualifiedPath = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    (fs, qualifiedPath)
  }

  /* Helper function to read from a binary file on S3 */
  def readFromFileOnDfs[T](session: SparkSession, path: String)(readFunc: FSDataInputStream => T): T = {

    val (fs, qualifiedPath) = asQualifiedPath(session, path)
    val input = fs.open(qualifiedPath)

    try {
      readFunc(input)
    } finally {
      if (input != null) {
        input.close()
      }
    }
  }
}
