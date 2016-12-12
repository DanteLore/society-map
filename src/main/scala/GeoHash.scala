// Taken from here: https://github.com/davidallsopp/geohash-scala

object GeoHash {
  val latitude_range = (-90.0, 90.0)
  val longitude_range = (-180.0, 180.0)
  val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
  val bits = Array(16, 8, 4, 2, 1)
  val toDec = Map(base32.zipWithIndex: _*)

  type Bounds = (Double, Double)
  private def mid(b: Bounds) = (b._1 + b._2) / 2.0
  implicit class BoundedNum(x: Double) { def in(b: Bounds): Boolean = x >= b._1 && x <= b._2 }

  private def bitsToChar(bin: Seq[Boolean]): Char = base32((bits zip bin).collect { case (x, true) => x }.sum)

  private def intToBits(i: Int) = (4 to 0 by -1) map (x => (i >> x & 1) == 1)

  private def isValid(s: String): Boolean = !s.isEmpty && s.forall(toDec.contains)

  private def toBits(s: String): Seq[Boolean] = s.flatMap(toDec andThen intToBits)

  def encode(lat: Double, lon: Double, precision: Int=12): String = {
    require(lat in latitude_range, "Latitude out of range")
    require(lon in longitude_range, "Longitude out of range")
    require(precision > 0, "Precision must be a positive integer")
    val rem = precision % 2 // if precision is odd, we need an extra bit so the total bits divide by 5
    val numbits = (precision * 5) / 2
    val latBits = findBits(lat, latitude_range, numbits)
    val lonBits = findBits(lon, longitude_range, numbits + rem)
    val bits = intercalate(lonBits, latBits)
    bits.grouped(5).map(bitsToChar).mkString
  }

  private def findBits(part: Double, bounds: Bounds, p: Int): List[Boolean] = {
    if (p == 0) Nil
    else {
      val avg = mid(bounds)
      if (part >= avg) true :: findBits(part, (avg, bounds._2), p - 1) // >= to match geohash.org encoding
      else false :: findBits(part, (bounds._1, avg), p - 1)
    }
  }

  def decode(hash: String): (Double, Double) = {
    require(isValid(hash), "Not a valid Base32 number")
    val (odd, even) = extracalate(toBits(hash))
    val lon = mid(decodeBits(longitude_range, odd))
    val lat = mid(decodeBits(latitude_range, even))
    (lat, lon)
  }

  private def decodeBits(bounds: Bounds, bits: Seq[Boolean]) =
    bits.foldLeft(bounds)((acc, bit) => if (bit) (mid(acc), acc._2) else (acc._1, mid(acc)))
  def intercalate[A](a: List[A], b: List[A]): List[A] = a match {
    case h :: t => h :: intercalate(b, t)
    case _ => b
  }

  def extracalate[A](a: Seq[A]): (List[A], List[A]) =
    a.foldRight((List[A](), List[A]())) { case (b, (a1, a2)) => (b :: a2, a1) }
}
