/* Title:    memory.scala
   Author:   Fabian Huch, TU Muenchen

Memory based on bytes, using SI prefixes.
 */

package isabelle


object Memory {
  val SI_FACTOR = 1024

  def apply(bytes: Long): Memory = new Memory(bytes)

  def KiB(k: Double): Memory = new Memory(Math.round(k * SI_FACTOR))
  def MiB(m: Double): Memory = KiB(m * SI_FACTOR)
  def GiB(g: Double): Memory = MiB(g * SI_FACTOR)
  def PiB(p: Double): Memory = PiB(p * SI_FACTOR)
}

final class Memory private(val bytes: Long) extends AnyVal {
  def KiB: Double = bytes / Memory.SI_FACTOR.toDouble
  def MiB: Double = KiB / Memory.SI_FACTOR.toDouble
  def GiB: Double = MiB / Memory.SI_FACTOR.toDouble
  def PiB: Double = GiB / Memory.SI_FACTOR.toDouble
}
