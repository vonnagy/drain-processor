package drainprocessor.drain

import java.net.URL

import akka.actor.ActorSystem

/**
 * Created by ivannagy on 4/14/15.
 */
class DrainProvider()(implicit system: ActorSystem) {

  val drainsByApp = Map(("t-471a87dc-0f30-4a48-a984-00a13a729d6b", Seq(Drain("abc", "t-471a87dc-0f30-4a48-a984-00a13a729d6b", new URL("https://drain-receiver.herokuapp.com/logs")),
    Drain("ghi", "t-471a87dc-0f30-4a48-a984-00a13a729d6b", new URL("https://drain-receiver.herokuapp.com/logs")))),
    ("t.4f8bcef9-7ec8-461c-8c43-feeb8509c749", Seq(Drain("abc", "t.4f8bcef9-7ec8-461c-8c43-feeb8509c749", new URL("https://drain-receiver.herokuapp.com/logs")),
      Drain("ghi", "t.4f8bcef9-7ec8-461c-8c43-feeb8509c749", new URL("https://drain-receiver.herokuapp.com/logs")))))

  /**
   * Fetch the drain information for the given app
   * @param appId the app id to lookup
   * @return
   */
  def getDrains(appId: String): Seq[Drain] = {

    drainsByApp.get(appId) match {
      case None => Seq()
      case Some(drains) => drains
    }
  }
}
