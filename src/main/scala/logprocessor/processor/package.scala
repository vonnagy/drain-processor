package drainprocessor

/**
 * Created by ivannagy on 4/10/15.
 */
package object processor {

  /**
   * This is the message that the processor manager will use to query the health
   * of each processor
   */
  case object CheckHealth

  /**
   * This is the message the a processor sends to itself when it is ready to accept
   * messages
   */
  case object ProcessorReady
}
