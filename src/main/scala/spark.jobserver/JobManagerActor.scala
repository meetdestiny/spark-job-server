package spark.jobserver

import akka.actor.{ActorRef, Props, PoisonPill}
import scala.concurrent.Future
import com.typesafe.config.Config
import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import ooyala.common.akka.InstrumentedActor
import org.apache.spark.{ SparkEnv, SparkContext }
import org.joda.time.DateTime
import scala.util.{Try , Success, Failure }
import spark.jobserver.ContextSupervisor.StopContext
import spark.jobserver.io.{ JobDAO, JobInfo, JarInfo }
import scala.concurrent.ExecutionContext.Implicits.global

object JobManagerActor {
  // Messages
  case object Initialize
  case class StartJob(appName: String, classPath: String, config: Config,
                      subscribedEvents: Set[Class[_]])

  // Results/Data
  case class Initialized(resultActor: ActorRef)
  case class InitError(t: Throwable)
  case class JobLoadingError(err: Throwable)
}

/**
 * The JobManager actor supervises jobs running in a single SparkContext, as well as shared metadata.
 * It creates a SparkContext.
 * It also creates and supervises a JobResultActor and JobStatusActor, although an existing JobResultActor
 * can be passed in as well.
 *
 * == Configuration ==
 * {{{
 *  num-cpu-cores = 4         # Total # of CPU cores to allocate across the cluster
 *  memory-per-node = 512m    # -Xmx style memory string for total memory to use for executor on one node
 *  dependent-jar-paths = ["local://opt/foo/my-foo-lib.jar"]   # URIs for dependent jars to load for entire context
 *  max-jobs-per-context = 4  # Max # of jobs to run at the same time
 *  coarse-mesos-mode = true  # per-context, rather than per-job, resource allocation
 *  rdd-ttl = 24 h            # time-to-live for RDDs in a SparkContext.  Don't specify = forever
 * }}}
 */
class JobManagerActor(dao: JobDAO,
                      contextName: String,
                      sparkMaster: String,
                      contextConfig: Config,
                      isAdHoc: Boolean,
                      resultActorRef: Option[ActorRef] = None) extends InstrumentedActor {

  
  import CommonMessages._
  import JobManagerActor._
  import scala.util.control.Breaks._
  import collection.JavaConverters._

  val config = context.system.settings.config

  var sparkContext: SparkContext = _
  var sparkEnv: SparkEnv = _
  protected var rddManagerActor: ActorRef = _

  private val maxRunningJobs = {
    val cpuCores = Runtime.getRuntime.availableProcessors
    Try(contextConfig.getInt("spark.jobserver.max-jobs-per-context")).getOrElse(cpuCores)
  }
  private val currentRunningJobs = new AtomicInteger(0)

  // When the job cache retrieves a jar from the DAO, it also adds it to the SparkContext for distribution
  // to executors.  We do not want to add the same jar every time we start a new job, as that will cause
  // the executors to re-download the jar every time, and causes race conditions.
  private val jobCacheSize = Try(config.getInt("spark.job-cache.max-entries")).getOrElse(10000)
  protected val jobCache = new JobCache(jobCacheSize, dao,
                                        { jarInfo => sparkContext.addJar(jarInfo.jarFilePath) })

  private val statusActor = context.actorOf(Props(new JobStatusActor(dao)), "status-actor")
  protected val resultActor = resultActorRef.getOrElse(context.actorOf(Props[JobResultActor], "result-actor"))

  override def postStop() {
    logger.info("Shutting down SparkContext {}", contextName)
    Option(sparkContext).foreach(_.stop())
  }

  def wrappedReceive: Receive = {
    case Initialize =>
      try {
        sparkContext = createContextFromConfig()
        sparkEnv = SparkEnv.get
        rddManagerActor = context.actorOf(Props(new RddManagerActor(sparkContext)), "rdd-manager-actor")
        getSideJars(contextConfig).foreach { jarPath => sparkContext.addJar(jarPath) }
        sender ! Initialized(resultActor)
      } catch {
        case t: Throwable =>
          logger.error("Failed to create context " + contextName + ", shutting down actor", t)
          sender ! InitError(t)
          self ! PoisonPill
      }

    case StartJob(appName, classPath, jobConfig, events) =>
      startJobInternal(appName, classPath, jobConfig, events, sparkContext, sparkEnv, rddManagerActor)
  }

  def startJobInternal(appName: String,
                       classPath: String,
                       jobConfig: Config,
                       events: Set[Class[_]],
                       sparkContext: SparkContext,
                       sparkEnv: SparkEnv,
                       rddManagerActor: ActorRef): Option[Future[Any]] = {
    var future: Option[Future[Any]] = None
    breakable {
      val lastUploadTime = dao.getLastUploadTime(appName)
      if (!lastUploadTime.isDefined) {
        sender ! NoSuchApplication
        break
      }

      // Check appName, classPath from jar
      val jarInfo = JarInfo(appName, lastUploadTime.get)
      val jobId = java.util.UUID.randomUUID().toString()
      logger.info("Loading class {} for app {}", classPath, appName: Any)
      val jobJarInfo = try {
        jobCache.getSparkJob(jarInfo.appName, jarInfo.uploadTime, classPath)
      } catch {
        case _: ClassNotFoundException =>
          sender ! NoSuchClass
          postEachJob()
          break
          null // needed for inferring type of return value
        case err: Throwable =>
          sender ! JobLoadingError(err)
          postEachJob()
          break
          null
      }

      // Automatically subscribe the sender to events so it starts getting them right away
      resultActor ! Subscribe(jobId, sender, events)
      statusActor ! Subscribe(jobId, sender, events)

      val jobInfo = JobInfo(jobId, contextName, jarInfo, classPath, DateTime.now(), None, None)
      future =
        Option(getJobFuture(jobJarInfo, jobInfo, jobConfig, sender, sparkContext, sparkEnv,
                            rddManagerActor))
    }

    future
  }

  private def getJobFuture(jobJarInfo: JobJarInfo,
                           jobInfo: JobInfo,
                           jobConfig: Config,
                           subscriber: ActorRef,
                           sparkContext: SparkContext,
                           sparkEnv: SparkEnv,
                           rddManagerActor: ActorRef): Future[Any] = {
    // Use the SparkContext's ActorSystem threadpool for the futures, so we don't corrupt our own
    implicit val executionContext = sparkEnv.actorSystem

    val jobId = jobInfo.jobId
    val constructor = jobJarInfo.constructor
    logger.info("Starting Spark job {} [{}]...", jobId: Any, jobJarInfo.className)

    // Atomically increment the number of currently running jobs. If the old value already exceeded the
    // limit, decrement it back, send an error message to the sender, and return a dummy future with
    // nothing in it.
    if (currentRunningJobs.getAndIncrement() >= maxRunningJobs) {
      currentRunningJobs.decrementAndGet()
      sender ! NoJobSlotsAvailable(maxRunningJobs)
      
      return Future[Any](None)
    }

    Future {
      org.slf4j.MDC.put("jobId", jobId)
      logger.info("Starting job future thread")

      // Need to re-set the SparkEnv because it's thread-local and the Future runs on a diff thread
      SparkEnv.set(sparkEnv)

      // Set the thread classloader to our special one, otherwise nested function classes don't load
      // Save the original classloader so we can restore it at the end, we don't want classloaders
      // hanging around
      val origLoader = Thread.currentThread.getContextClassLoader()
      Thread.currentThread.setContextClassLoader(jobJarInfo.loader)
      val job = constructor()
      if (job.isInstanceOf[NamedRddSupport]) {
        val namedRdds = job.asInstanceOf[NamedRddSupport].namedRddsPrivate
        if (namedRdds.get() == null) {
          namedRdds.compareAndSet(null, new JobServerNamedRdds(rddManagerActor))
        }
      }

      try {
        statusActor ! JobStatusActor.JobInit(jobInfo)

        job.validate(sparkContext, jobConfig) match {
          case SparkJobInvalid(reason) => {
            val err = new Throwable(reason)
            statusActor ! JobValidationFailed(jobId, DateTime.now(), err)
            throw err
          }
          case SparkJobValid => {
            statusActor ! JobStarted(jobId: String, contextName, jobInfo.startTime)
            job.runJob(sparkContext, jobConfig)
          }
        }
      } finally {
        Thread.currentThread.setContextClassLoader(origLoader)
        org.slf4j.MDC.remove("jobId")
      }
    }.andThen {
      case Success(result: Any) =>
        statusActor ! JobFinished(jobId, DateTime.now())
        resultActor ! JobResult(jobId, result)
      case Failure(error: Throwable) =>
        // If and only if job validation fails, JobErroredOut message is dropped silently in JobStatusActor.
        statusActor ! JobErroredOut(jobId, DateTime.now(), error)
        logger.warn("Exception from job " + jobId + ": ", error)
    }.andThen {
      case _ =>
        // Make sure to decrement the count of running jobs when a job finishes, in both success and failure
        // cases.
        resultActor ! Unsubscribe(jobId, subscriber)
        statusActor ! Unsubscribe(jobId, subscriber)
        currentRunningJobs.getAndDecrement()
        postEachJob()
    }
  }

  def createContextFromConfig(contextName: String = contextName): SparkContext = {
    for (cores <- Try(contextConfig.getInt("num-cpu-cores"))) {
      System.setProperty("spark.cores.max", cores.toString)
    }
    // Should be a -Xmx style string eg "512m", "1G"
    for (nodeMemStr <- Try(contextConfig.getString("memory-per-node"))) {
      System.setProperty("spark.executor.memory", nodeMemStr)
    }

    val sparkHome: String = Try(config.getString("spark.home")).getOrElse(null)

    // Set number of akka threads
    // TODO(ilyam): need to figure out how many extra threads spark needs, besides the job threads
    System.setProperty("spark.akka.threads", (maxRunningJobs + 4).toString)

    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")

    // Set the Jetty port to 0 to find a random port
    System.setProperty("spark.ui.port", "0")

    val coarseMesos = Try(contextConfig.getBoolean("coarse-mesos-mode")).getOrElse(false)
    System.setProperty("spark.mesos.coarse", coarseMesos.toString)

    // TTL for cleaning cached RDDs
    Try(contextConfig.getMilliseconds("rdd-ttl")).foreach { ttl =>
      System.setProperty("spark.cleaner.ttl", (ttl / 1000L).toString)
    }

    new SparkContext(sparkMaster, contextName, sparkHome)
  }

  // This method should be called after each job is succeeded or failed
  private def postEachJob() {
    // Delete the JobManagerActor after each adhoc job
    if (isAdHoc)
      context.parent ! StopContext(contextName) // its parent is LocalContextSupervisorActor
  }

  // "Side jars" are jars besides the main job jar that are needed for running the job.
  // They are loaded from the context/job config.
  // Each one should be an URL (http, ftp, hdfs, local, or file). local URLs are local files
  // present on every node, whereas file:// will be assumed only present on driver node
  private def getSideJars(config: Config): Seq[String] =
    Try(config.getStringList("dependent-jar-paths").asScala.toSeq).getOrElse(Nil)
}
