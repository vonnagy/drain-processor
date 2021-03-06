package drainprocessor.processor

import java.util.concurrent.TimeoutException

import akka.actor._
import akka.pattern.ask
import akka.routing.FromConfig
import akka.util.Timeout
import com.github.vonnagy.service.container.health.{HealthInfo, HealthState, RegisteredHealthCheckActor}
import com.github.vonnagy.service.container.http.routing.RoutedService
import com.github.vonnagy.service.container.log.ActorLoggingAdapter
import drainprocessor.drain.Drainer

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}


object ProcessorManager {
  def props(): Props = Props[ProcessorManager]
}

class ProcessorManager extends Actor with RegisteredHealthCheckActor with ActorLoggingAdapter {

  import SupervisorStrategy._
  import context.dispatcher

  val drainers = context.actorOf(FromConfig.props(Props[Drainer]), "drainer")

  // TODO customize
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case _: ActorInitializationException => Stop
      case _: IllegalArgumentException => Stop
      case _: IllegalStateException => Restart
      case _: TimeoutException => Escalate
      case _: Exception => Escalate
    }

  override def preStart() {
    log.info("Processor manager started: {}", context.self.path)
    startProcessors
  }

  override def postStop(): Unit = {
    context.children.foreach(context.stop(_))
    log.info("Processor manager stopped: {}", context.self.path)
  }

  override def receive = {
    case ProcessorReady =>
      // A processor is ready to go
  }

  /**
   * Start the drainers
   */
  private def startDrainers: Unit = {

  }

  /**
   * Load the defined processors
   */
  private def startProcessors: Seq[ActorRef] = {
    val master = context.system.settings.config.getConfig("log.processors")

    master.root().entrySet().flatMap { entry =>
      try {
        if (master.getConfig(entry.getKey).getBoolean("enabled")) {
          var config = master.getConfig(entry.getKey)
          var clazz = config.getString("class");

          val processor = context.actorOf(Props(Class.forName(clazz), drainers), entry.getKey.toLowerCase)
          //processor.asInstanceOf[Processor].setDrainer(drainers)
          Some(processor)
        }
        else {
          None
        }

      } catch {
        case ex: Exception =>
          log.error(s"Log processor specified in config can't be loaded [${entry}]", ex);
          None
      }
    }.toSeq

  }

  /**
   * Fetch the health for this registered checker.
   *
   * @return returns a the health information
   */
  override def getHealth: Future[HealthInfo] = {

    if (context.children.isEmpty) {
      Future { new HealthInfo("processors", HealthState.DEGRADED, s"There are no configured processors running") }
    }
    else {
      implicit val timeout = Timeout(2 seconds)
      val future = (Future.traverse(context.children)(x => x ? CheckHealth)).mapTo[Seq[HealthInfo]]

      val p = Promise[HealthInfo]()
      future.onComplete({
        case Failure(f) =>
          log.error("Error fetching health", f)
          p success new HealthInfo("processors", HealthState.DEGRADED, s"We were unable to determine the health on the processors")
        case Success(answers) =>
          p.success(HealthInfo("processors", HealthState.OK,
            s"The following processors are currently running: ${context.children.map(_.path.name).mkString(", ")}",
            null, answers.toList))
      })

      p.future
    }

  }
}