package spark.jobserver

import org.joda.time.DateTime
import spark.jobserver.io.JobDAO
import spark.jobserver.util.LRUCache

case class JobJarInfo(constructor: () => SparkJob,
                      className: String,
                      jarFilePath: String,
                      loader: ClassLoader)

/**
 * A cache for SparkJob classes.  A lot of times jobs are run repeatedly, and especially for low-latency
 * jobs, why retrieve the jar and load it every single time?
 */
class JobCache(maxEntries: Int, dao: JobDAO, retrievalFunc: JobJarInfo => Unit) {
  private val cache = new LRUCache[(String, DateTime, String), JobJarInfo](maxEntries)

  /**
   * Retrieves the given SparkJob class from the cache if it's there, otherwise use the DAO to retrieve it.
   * @param appName the appName under which the jar was uploaded
   * @param uploadTime the upload time for the version of the jar wanted
   * @param classPath the fully qualified name of the class/object to load
   */
  def getSparkJob(appName: String, uploadTime: DateTime, classPath: String): JobJarInfo = {
    cache.get((appName, uploadTime, classPath), {
      val jarFilePath = dao.retrieveJarFile(appName, uploadTime)
      val (constructor, loader) = JarUtils.loadClassOrObjectFromJar[SparkJob](classPath, jarFilePath)
      val info = JobJarInfo(constructor, classPath, new java.io.File(jarFilePath).getAbsolutePath(), loader)
      retrievalFunc(info)
      info
    })
  }
}
