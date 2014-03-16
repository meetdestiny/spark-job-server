package spark.jobserver

import akka.actor.ActorRef
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import ooyala.common.akka.InstrumentedActor
import spark.jobserver.ContextSupervisor.{GetContext, GetAdHocContext}
import spark.jobserver.io.JobDAO
import scala.concurrent.ExecutionContext.Implicits.global

object JobInfoActor {
  case class GetJobStatuses(limit: Option[Int])
}

class JobInfoActor(jobDao: JobDAO, contextSupervisor: ActorRef) extends InstrumentedActor {
  import scala.concurrent.duration._
  import CommonMessages._
  import JobInfoActor._
  import scala.util.control.Breaks._

  // Used in the asks (?) below to request info from contextSupervisor and resultActor
  implicit val ShortTimeout = Timeout(3 seconds)

  override def wrappedReceive: Receive = {
    case GetJobStatuses(limit) =>
      val infos = jobDao.getJobInfos.values.toSeq.sortBy(_.startTime.toString())
      if (limit.isDefined) {
        sender ! infos.takeRight(limit.get)
      } else {
        sender ! infos
      }

    case GetJobResult(jobId) =>
      breakable {
        val jobInfoOpt = jobDao.getJobInfos.get(jobId)
        if (!jobInfoOpt.isDefined) {
          sender ! NoSuchJobId
          break
        }

        jobInfoOpt.filter { job => job.isRunning || job.isErroredOut }
          .foreach { jobInfo =>
            sender ! jobInfo
            break
          }

        // get the context from jobInfo
        val context = jobInfoOpt.get.contextName

        val future = (contextSupervisor ? ContextSupervisor.GetResultActor(context)).mapTo[ActorRef]
        val resultActor = Await.result(future, 3 seconds)

        val receiver = sender // must capture the sender since callbacks are run in a different thread
        for (result <- (resultActor ? GetJobResult(jobId))) {
          receiver ! result // a JobResult(jobId, result) object is sent
        }
      }
  }
}
