package dqsuite.utils

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

private[dqsuite] object RuntimeCompileUtils {
  private val toolbox = currentMirror.mkToolBox()

  private[dqsuite] def evaluate(source: String): Any = {
    toolbox.eval(toolbox.parse(source))
  }
}
