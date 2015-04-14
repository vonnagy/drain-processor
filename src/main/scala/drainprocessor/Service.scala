package drainprocessor

import com.github.vonnagy.service.container.ContainerBuilder
import drainprocessor.processor.ProcessorManager

/**
 * Created by ivannagy on 4/8/15.
 */
object Service extends App {

  // Here we establish the container and build it while
  // applying extras.
  val service = new ContainerBuilder()
    .withActors(("processor-manager", ProcessorManager.props())).build

  service.start

}
