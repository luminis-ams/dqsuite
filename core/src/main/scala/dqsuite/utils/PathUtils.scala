package dqsuite.utils

object PathUtils {
  def ensureTrailingSlash(path: String): String = {
    if (path.endsWith("/")) {
      path
    } else {
      s"$path/"
    }
  }
}
