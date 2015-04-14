package drainprocessor.processor.kinesis

import java.util.concurrent.TimeUnit

import com.github.vonnagy.service.container.health.{HealthInfo, HealthState}
import drainprocessor.processor.{Processor, ProcessorReady}

/**
 * Created by ivannagy on 4/10/15.
 */
class KinesisProcessor extends Processor {

  val timeout = context.system.settings.config.getDuration("log.processors.kinesis.timeout", TimeUnit.MILLISECONDS).toInt
  var connected = false
  lazy val streams = verifyStreams

  def lineMetricPrefix = "processors.kinesis"

  override def preStart() {

    super.preStart
    streams.get("log-stream").get.start
    self ! ProcessorReady
  }

  override def postStop() {

    log.info("Kinesis processor stopping: {}", context.self.path)
    streams.get("log-stream").get.stop
    connected = false

    super.postStop
  }

  def running: Receive = {
    case _ =>
  }

  def getHealth: HealthInfo = connected match {
    case true =>
      new HealthInfo("kinesis", HealthState.OK, s"The processor running and attached to kinesis")
    case false =>
      new HealthInfo("kinesis", HealthState.DEGRADED, s"The processor is running, but can't attach to kinesis")
  }

  /**
   * Make sure the the proper streams are up and running before registering or accepting any log work
   */
  def verifyStreams(): Map[String, StreamManager] = {

    log.info("Locating the streams {} and {}", "log-stream")
    Map(("log-stream", new StreamManager("log-stream")))

  }

}
