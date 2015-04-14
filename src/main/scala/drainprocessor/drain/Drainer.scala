package drainprocessor.drain

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Cancellable}
import com.amazonaws.services.kinesis.model.Record
import com.github.vonnagy.service.container.log.ActorLoggingAdapter
import com.github.vonnagy.service.container.metrics.Counter
import drainprocessor._
import spray.client.pipelining.sendReceive
import spray.http.HttpHeaders.RawHeader
import spray.http.{HttpEntity, HttpHeaders}
import spray.httpx.RequestBuilding

import scala.concurrent.duration.Duration

/**
 * Created by ivannagy on 4/14/15.
 */
class Drainer extends Actor with ActorLoggingAdapter with RequestBuilding {

  import context.dispatcher

  val maxBundleSize = context.system.settings.config.getInt("log.drains.max-bundle-size")
  val bundleTimeout = context.system.settings.config.getDuration("log.drains.bundle-timeout", TimeUnit.MILLISECONDS)

  val drainProvider = new DrainProvider()(context.system)
  val droppedRecords = Counter(s"drainers.no-drain")(context.system)

  var appMap = Map[String, AppData]()

  class AppData(appId: String) {
    import context.system
    val id = appId
    val receivedCount = Counter(s"drainers.${appId.replace(".", "-")}.receive")
    val pumpCount = Counter(s"drainers.${appId.replace(".", "-")}.pump")
    var scheduledTimeout: Option[Cancellable] = None
    var bundle = Seq[String]()
  }

  case class Timeout(appId: String)

  val clientPipeline = sendReceive

  override def preStart() {
    log.info("Drainer starting: {}", context.self.path)
    context.system.eventStream.subscribe(self, classOf[Record])
    super.preStart
  }

  override def postStop() {

    log.info("Drainer stopping: {}", context.self.path)
    context.system.eventStream.unsubscribe(self, classOf[Record])
    super.postStop
  }

  def receive = {
    case r: Record =>
      val appId = r.getPartitionKey
      drainProvider.getDrains(appId).size match {
        case 0 => droppedRecords.incr
        case _ => processRecord(r)
      }

    case Timeout(appId) =>
      pumpDrains(appMap.get(appId).get)
  }

  def processRecord(rec: Record): Unit = {

    val app = appMap.get(rec.getPartitionKey) match {
      case None => new AppData(rec.getPartitionKey)
      case a => a.get
    }

    app.receivedCount.incr
    val line = new String(rec.getData.array());

    app.bundle = app.bundle :+ line
    appMap = appMap.updated(app.id, app)

    if (app.bundle.size == maxBundleSize) {
      pumpDrains(app)
    }
    else if (app.scheduledTimeout.isEmpty) {
      app.scheduledTimeout = Some(context.system.scheduler.scheduleOnce(Duration(bundleTimeout, TimeUnit.MILLISECONDS), self, Timeout(rec.getPartitionKey)))
      appMap = appMap.updated(app.id, app)
    }
  }

  def pumpDrains(app: AppData): Unit = {

    drainProvider.getDrains(app.id) foreach { drain =>
      val url = drain.endpoint

      // Send to drain
      val startTimestamp = System.currentTimeMillis()
      val response = clientPipeline {
        Post(url.toString).withHeaders(RawHeader(`Logplex-Msg-Count`, app.bundle.size.toString),
          RawHeader(`Logplex-Drain-Token`, drain.drainId),
          RawHeader(`Logplex-Frame-Id`, UUID.randomUUID().toString),
          HttpHeaders.`Content-Type`(`application/logplex-1`)).withEntity(HttpEntity(app.bundle.mkString("")))
      }

      response.onComplete { x =>
        app.pumpCount.incr
        log.info(s"Request to ${app.id} completed in ${System.currentTimeMillis() - startTimestamp} millis.")
      }
    }

    if (app.scheduledTimeout.isDefined)
      app.scheduledTimeout.get.cancel()

    app.bundle = Seq[String]()
    appMap = appMap.updated(app.id, app)
  }
}
