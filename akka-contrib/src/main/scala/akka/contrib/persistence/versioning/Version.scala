package akka.contrib.persistence.versioning

import scala.util.Try

case class Version(major: Int = 0, minor: Int = 0, micro: Int = 0, qualifier: Option[String] = None, build: Option[Int] = None) extends Ordered[Version] {
  private[this] def format(qualifierDelimiter: String) = s"$major.$minor.$micro${qualifier.map(qualifierDelimiter + _ + build.map("-" + _).getOrElse("")).getOrElse("")}"

  def toOsgiString = format(".")
  def toMavenString = format("-")
  override def toString = toMavenString

  private[this] val Last = Char.MaxValue.toString

  override def compare(that: Version): Int = {
    def fix(q: String) = if (q == "") Last else q
    implicitly[Ordering[Option[(Int, Int, Int, Option[String], Option[Int])]]].compare(
      Version.unapply(this.copy(qualifier = this.qualifier.map(fix))),
      Version.unapply(that.copy(qualifier = that.qualifier.map(fix))))
  }
}
object Version {
  private[this] val VersionRe = """(?:(\d+)(?:\.(\d+)(?:\.(\d+)(?:(?:\.([\w_-]+))|(?:-([\w_-]+?)(?:-(\d+))?))?)?)?)?""".r
  def apply(literal: String): Option[Version] = {
    Try {
      val VersionRe(major, minor, micro, osgiQualifier, mavenQualifier, mavenBuild) = literal
      Option(major).map { m ⇒
        Version(major.toInt, Option(minor).map(_.toInt).getOrElse(0), Option(micro).map(_.toInt).getOrElse(0), Option(osgiQualifier).orElse(Option(mavenQualifier)), Option(mavenBuild).map(_.toInt))
      }.getOrElse(Version.Zero)
    }.toOption
  }
  val Zero = Version()
  object Major {
    def unapply(v: Version): Option[Int] = Option(v).map(_.major)
  }
  object Minor {
    def unapply(v: Version): Option[(Int, Int)] = Option(v).map(v ⇒ (v.major, v.minor))
  }
  object Micro {
    def unapply(v: Version): Option[(Int, Int, Int)] = Option(v).map(v ⇒ (v.major, v.minor, v.micro))
  }
  object Qualifier {
    def unapply(v: Version): Option[(Int, Int, Int, String)] = Option(v).map(v ⇒ (v.major, v.minor, v.micro, v.qualifier.getOrElse("")))
  }
  object Build {
    def unapply(v: Version): Option[(Int, Int, Int, String, Int)] = Option(v).map(v ⇒ (v.major, v.minor, v.micro, v.qualifier.getOrElse(""), v.build.getOrElse(0)))
  }

}