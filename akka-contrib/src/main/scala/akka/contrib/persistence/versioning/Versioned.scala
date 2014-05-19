package akka.contrib.persistence.versioning

import akka.actor.{ ExtendedActorSystem, ActorSystem }
import akka.serialization.{ SerializationExtension, Serializer }
import scala.util.{ Try, Failure, Success }
import scala.reflect.runtime.{ currentMirror ⇒ cm }

/**
 * Versioned is a marker interface used to signal that the extending class or trait should be persisted by converting
 * the class state to a product and persisting that product along with a version identifier. This allows the companion
 * object of the class to handle the deserialization in different ways depending on the version identifier found in each
 * case.
 * <p/>
 * For example:
 * <code>
 *   // Version 1
 *   case class Name(first: String, last: String) extends Versioned[Name]
 *
 *   // Version 2
 *   case class Name(first: String, last: String, title: Option[String] = None) extends Versioned[Name]
 *
 *   object Name extends Versioned.Companion[Name] {
 *     override def apply(data: (Version, Any)): Name = {
 *       case (_, None) => null
 *       case (Version.Major(1), Some((first: String, last: String))) => Name(first, last)
 *       case (Version.Major(2), Some((first: String, last: String, title: Option[String]))) => Name(first, last, title)
 *     }
 *   }
 * </code>
 * @tparam T
 */
trait Versioned[T <: AnyRef] extends Serializable { self: T ⇒
  lazy val companion: Versioned.Companion[T] = Versioned.Companion.load[T](getClass.asInstanceOf[Class[T]])
  def tuple: Product = companion.unapply(this).orNull
}
object Versioned {
  trait Companion[T <: AnyRef] {
    def version: Version = null
    def apply(data: (Version, Product)): T
    def unapply(t: T): Option[Product]
  }
  object Companion {
    def load[T <: AnyRef](c: Class[T]): Companion[T] = {
      def findVersionedClass(candidates: Seq[Class[_]]): Option[Class[T]] = {
        if (candidates.isEmpty) None
        else {
          val head = candidates.head
          if (head.getInterfaces.contains(classOf[Versioned[T]])) Some(head.asInstanceOf[Class[T]])
          else findVersionedClass(candidates.tail ++ Seq(head.getSuperclass) ++ head.getInterfaces)
        }
      }
      findVersionedClass(Seq(c)).map {
        vc ⇒
          Try {
            val classSymbol = cm.classSymbol(vc)
            val moduleSymbol = classSymbol.companionSymbol.asModule
            val moduleMirror = cm.reflectModule(moduleSymbol)
            moduleMirror.instance.asInstanceOf[Companion[T]]
          }.getOrElse(throw new NotImplementedError("The companion object for " + vc.getName + " must implement Versioned.Companion"))
      }.getOrElse(throw new NotImplementedError("No super class or interface implementing Versioned found for " + c.getName))
    }
  }
}

class VersionedSerializer(system: ExtendedActorSystem) extends Serializer {
  lazy val serialization = SerializationExtension(system)

  def includeManifest: Boolean = true

  def identifier = 740115

  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case v: Versioned[_] ⇒
      val data = (version(v.companion.version), v.tuple)
      val serializer = serialization.findSerializerFor(data)
      serializer.toBinary(data)
  }

  def fromBinary(
    bytes: Array[Byte],
    clazz: Option[Class[_]]): AnyRef = {
    val c = clazz.get.asInstanceOf[Class[_ <: AnyRef]]
    val companion = Versioned.Companion.load(c)
    serialization.deserialize(bytes, classOf[(Version, _)]) match {
      case Success(d) if (d.isInstanceOf[(Version, Product)]) ⇒ {
        val data = d.asInstanceOf[(Version, Product)]
        companion.apply(data)
      }
      case Failure(ex) ⇒ throw ex
      case x           ⇒ throw new IllegalStateException("Unknown result when deserializing object: " + x)
    }
  }

  private[this] def version(v: Version): Version = {
    if (v != null) v
    else Try(system.settings.config.getString("akka.contrib.persistence.versioned.version")).toOption.flatMap(Version.apply).getOrElse(Version.Zero)
  }
}
