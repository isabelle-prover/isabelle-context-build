/* Title:    memory.scala
   Author:   Fabian Huch, TU Muenchen

Memory based on bytes.
 */

package isabelle


object Memory {
  def kib(k: Long): Memory = new Memory(k * 1024)
  def gib(g: Long): Memory = kib(g * 1024)
}

final class Memory private(val bytes: Long) extends AnyVal {
  def kib: Double = bytes / 1024.0
}
