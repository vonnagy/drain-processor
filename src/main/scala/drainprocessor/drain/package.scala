package drainprocessor

import java.net.URL

/**
 * Created by ivannagy on 4/14/15.
 */
package object drain {

  case class Drain(drainId: String, appId: String, endpoint: URL)

}
