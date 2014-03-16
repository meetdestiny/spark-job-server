package spark.jobserver

import java.io.File
import java.lang.ClassLoader
import java.lang.reflect.Constructor
import java.net.{ URL, URLClassLoader }
import org.slf4j.LoggerFactory

/**
 *  A set of utilities for dynamically loading classes from a jar file, and saving the jar file.
 */
object JarUtils {
  val logger = LoggerFactory.getLogger(getClass)

  /**
   * Loads a Scala object or class from the jarPath.
   * See http://stackoverflow.com/questions/3216780/problem-reloading-a-jar-using-urlclassloader?lq=1
   * See http://stackoverflow.com/questions/8867766/scala-dynamic-object-class-loading
   *
   *
   * @param classOrObjectName must be the fully qualified name of the Scala object or class that
   *                          implements the SparkJob trait. If an object is used, do not include the
   *                          trailing '$'.
   * @param jarPath path to the jar file.
   * @return a 2-tuple of (Function0[C], ClassLoader). Calling the function will return a reference to
   *         the object (for objects), or a new instance of a class (for classes) that implement the
   *         SparkJob trait.
   */
  def loadClassOrObjectFromJar[C](classOrObjectName: String, jarPath: String): (() => C, ClassLoader) = {
    val jarUrl = new File(jarPath).getAbsoluteFile.toURI.toURL
    val loader = new URLClassLoader(Array(jarUrl), getClass.getClassLoader())

    def fallBackToClass(): (() => C, ClassLoader) = {
      val constructor = loadConstructor[C](classOrObjectName, jarUrl, loader)
      (() => constructor.newInstance(), loader)
    }

    // Try loading it as an object first, if that fails, then try it as a class
    try {
      val objectRef = loadObject[C](classOrObjectName + "$", jarUrl, loader)
      (() => objectRef, loader)
    } catch {
      case e: java.lang.ClassNotFoundException => fallBackToClass()
      case e: java.lang.ClassCastException => fallBackToClass()
      case e: java.lang.NoSuchMethodException => fallBackToClass()
      case e: java.lang.NoSuchFieldException => fallBackToClass()
    }
  }

  private def loadConstructor[C](className: String, jarUrl: URL, loader: ClassLoader): Constructor[C] = {
    logger.info("Loading class {} from URL {}, using loader {}", className, jarUrl.toString, loader)
    val loadedClass = loader.loadClass(className).asInstanceOf[Class[C]]
    val result = loadedClass.getConstructor()
    if (loadedClass.getClassLoader != loader) {
      logger.error("Wrong ClassLoader for class {}: Expected {} but got {}", loadedClass.getName,
        loader.toString, loadedClass.getClassLoader.toString)
    }
    result
  }

  private def loadObject[C](objectName: String, jarUrl: URL, loader: ClassLoader): C = {
    logger.info("Loading object {} from URL {}, using loader {}", objectName, jarUrl.toString, loader)
    val loadedClass = loader.loadClass(objectName)
    val objectRef = loadedClass.getField("MODULE$").get(null).asInstanceOf[C]
    if (objectRef.getClass.getClassLoader != loader) {
      logger.error("Wrong ClassLoader for object {}: Expected {} but got {}", objectRef.getClass.getName,
        loader.toString, objectRef.getClass.getClassLoader.toString)
    }
    objectRef
  }

  def validateJarBytes(jarBytes: Array[Byte]): Boolean = {
    jarBytes.size > 4 &&
      // For now just check the first few bytes are the ZIP signature: 0x04034b50 little endian
      jarBytes(0) == 0x50 && jarBytes(1) == 0x4b && jarBytes(2) == 0x03 && jarBytes(3) == 0x04
  }
}
