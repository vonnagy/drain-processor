package drainprocessor.processor.kinesis

import java.net.InetAddress
import java.util.UUID

import akka.actor.ActorContext
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.kinesis.clientlibrary.exceptions.{InvalidStateException, ShutdownException, ThrottlingException}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory
import com.amazonaws.services.kinesis.model.Record
import com.github.vonnagy.service.container.log.LoggingAdapter
import com.github.vonnagy.service.container.metrics.Counter

import scala.collection.JavaConversions._
import scala.util.control.Breaks

/**
 * Created by ivannagy on 4/12/15.
 */
class StreamManager(name: String)(implicit context: ActorContext) extends LoggingAdapter {

  import context.system

  val mybreaks = new Breaks

  import mybreaks.{break, breakable}

  val endpoint = context.system.settings.config.getString("log.processors.kinesis.endpoint")
  val accessKey = context.system.settings.config.getString("log.processors.kinesis.access-key")
  val accessSecret = context.system.settings.config.getString("log.processors.kinesis.access-secret")
  val initPos = context.system.settings.config.getString(s"log.processors.kinesis.streams.$name.initial-position")

  var worker: Option[Worker] = None

  val credentials = new AWSCredentials {
    def getAWSAccessKeyId: String = accessKey

    def getAWSSecretKey: String = accessSecret
  }

  val receivedCount = Counter("processors.kinesis.receive")

  /**
   * Never-ending processing loop over source stream.
   */
  def start {

    val workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
    log.info("Using workerId: " + workerId)

    val kinesisClientLibConfiguration = new KinesisClientLibConfiguration("drain-processor", name,
      new CredentialsProvider(credentials), workerId).
      withKinesisEndpoint(endpoint).
      withInitialPositionInStream(InitialPositionInStream.valueOf(initPos))

    log.info(s"Running: drain-processor.")

    val rawEventProcessorFactory = new RawEventProcessorFactory()
    worker = Some(new Worker(
      rawEventProcessorFactory,
      kinesisClientLibConfiguration,
      new NullMetricsFactory()
    ))

    context.dispatcher.execute(worker.get)
  }

  def stop: Unit = {
    if (worker.isDefined)
      worker.get.shutdown
  }

  // Factory needed by the Amazon Kinesis Consumer library to
  // create a processor.
  class RawEventProcessorFactory()
    extends IRecordProcessorFactory {
    @Override
    def createProcessor: IRecordProcessor = {
      return new RawEventProcessor();
    }
  }

  // Process events from a Kinesis stream.
  class RawEventProcessor()
    extends IRecordProcessor {

    private var kinesisShardId: String = _
    private var nextCheckpointTimeInMillis: Long = _

    // Backoff and retry settings.
    private val BACKOFF_TIME_IN_MILLIS = 3000L
    private val NUM_RETRIES = 10
    private val CHECKPOINT_INTERVAL_MILLIS = 1000L

    override def initialize(shardId: String) = {
      log.info("Initializing record processor for shard: " + shardId)
      this.kinesisShardId = shardId
    }

    override def processRecords(records: java.util.List[Record],
                                checkpointer: IRecordProcessorCheckpointer) = {

      log.debug(s"Processing ${records.size} records from $kinesisShardId")
      processRecordsWithRetries(records)

      if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
        checkpoint(checkpointer)
        nextCheckpointTimeInMillis =
          System.currentTimeMillis + CHECKPOINT_INTERVAL_MILLIS
      }
    }

    private def processRecordsWithRetries(records: java.util.List[Record]) = {
      for (record <- records) {
        try {
          log.trace(s"Sequence number: ${record.getSequenceNumber}")
          log.trace(s"Partition key: ${record.getPartitionKey}")
          receivedCount.incr
        } catch {
          case t: Throwable =>
            log.error(s"Caught throwable while processing record $record", t)
        }
      }
    }

    override def shutdown(checkpointer: IRecordProcessorCheckpointer,
                          reason: ShutdownReason) = {

      log.info(s"Shutting down record processor for shard: $kinesisShardId")
      if (reason == ShutdownReason.TERMINATE) {
        checkpoint(checkpointer)
      }
    }

    private def checkpoint(checkpointer: IRecordProcessorCheckpointer) = {
      log.info(s"Checkpointing shard $kinesisShardId")
      breakable {
        for (i <- 0 to NUM_RETRIES - 1) {
          try {
            checkpointer.checkpoint()
            break
          } catch {
            case se: ShutdownException =>
              log.info("Caught shutdown exception, skipping checkpoint.", se)
            case e: ThrottlingException =>
              if (i >= (NUM_RETRIES - 1)) {
                log.info(s"Checkpoint failed after ${i + 1} attempts.", e)
              } else {
                log.info(s"Transient issue when checkpointing - attempt ${i + 1} of "
                  + NUM_RETRIES, e)
              }
            case e: InvalidStateException =>
              log.info("Cannot save checkpoint to the DynamoDB table used by " +
                "the Amazon Kinesis Client Library.", e)
          }
          Thread.sleep(BACKOFF_TIME_IN_MILLIS)
        }
      }
    }
  }

}
