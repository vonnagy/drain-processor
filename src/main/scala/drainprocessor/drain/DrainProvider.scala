package drainprocessor.drain

import java.net.URL

import akka.actor.ActorSystem
import com.github.vonnagy.service.container.log.LoggingAdapter

/**
 * Created by ivannagy on 4/14/15.
 */
class DrainProvider()(implicit system: ActorSystem) extends LoggingAdapter {

  var drainsByApp = Map[String, Seq[Drain]]()
  /**
   * Fetch the drain information for the given app
   * @param appId the app id to lookup
   * @return
   */
  def getDrains(appId: String): Seq[Drain] = {

    drainsByApp.get(appId) match {
      case None =>
        if (drainsByApp.size < 5) {
          drainsByApp = drainsByApp.updated(appId, Seq(Drain(appId, appId, new URL("https://drain-receiver.herokuapp.com/logs"))))
        }
        Seq()
      case Some(drains) => drains
    }
  }
}
