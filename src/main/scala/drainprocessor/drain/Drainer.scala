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
  var receivedCount: Option[Counter] = None
  var pumpCount: Option[Counter] = None
  var scheduledTimeout: Option[Cancellable] = None
  var bundle = Seq[String]()
  var appId: Option[String] = None

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
      if (this.appId.isEmpty)
        this.appId = Some(appId)

      drainProvider.getDrains(appId).size match {
        case 0 => droppedRecords.incr
        case _ => processRecord(r)
      }

    case "timeout" =>
      pumpDrains(bundle)
  }

  def processRecord(rec: Record): Unit = {

    if (receivedCount.isEmpty) {
      import context.system
      receivedCount = Some(Counter(s"drainers.${appId.get.replace(".", "-")}.receive"))
      pumpCount = Some(Counter(s"drainers.${appId.get.replace(".", "-")}.pump"))
    }
   
    receivedCount.get.incr
    val line = new String(rec.getData.array());

    bundle = bundle ++ Seq(line)
    if (bundle.size == maxBundleSize) {
      pumpDrains(bundle)
    }
    else if (scheduledTimeout.isEmpty) {
      context.system.scheduler.scheduleOnce(Duration(bundleTimeout, TimeUnit.MILLISECONDS), self, "timeout")
    }
  }

  def pumpDrains(data: Seq[String]): Unit = {

    drainProvider.getDrains(appId.get) foreach { drain =>
      val url = drain.endpoint

      // Send to drain
      val startTimestamp = System.currentTimeMillis()
      val response = clientPipeline {
        Post(url.toString).withHeaders(RawHeader(`Logplex-Msg-Count`, data.size.toString),
          RawHeader(`Logplex-Drain-Token`, drain.drainId),
          RawHeader(`Logplex-Frame-Id`, UUID.randomUUID().toString),
          HttpHeaders.`Content-Type`(`application/logplex-1`)).withEntity(HttpEntity(data.mkString("")))
      }

      response.onComplete { x =>
        pumpCount.get.incr
        log.info(s"Request to ${this.appId.get} completed in ${System.currentTimeMillis() - startTimestamp} millis.")
      }
    }

    if (scheduledTimeout.isDefined)
      scheduledTimeout.get.cancel()

    bundle = Seq[String]()
  }
}
