/* Title: buffered_progress.scala
   Author: Fabian Huch, TU Muenchen

Buffered implementation for progress.
 */
package isabelle

class Buffered_Progress extends Progress {
  private val _out = new StringBuffer(256)
  private val _err = new StringBuffer(256)

  def out: String = _out.toString
  def err: String = _err.toString

  override def echo(msg: String): Unit = synchronized(_out.append(msg))

  override def echo_warning(msg: String): Unit =
    synchronized(_err.append(Output.warning_text(msg)))

  override def echo_error_message(msg: String): Unit =
    synchronized(_err.append(Output.error_message_text(msg)))
}