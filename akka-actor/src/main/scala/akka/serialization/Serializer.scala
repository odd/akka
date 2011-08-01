package akka.serialization

/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

import java.io.{ ObjectOutputStream, ByteArrayOutputStream, ObjectInputStream, ByteArrayInputStream }
import akka.util.ClassLoaderObjectInputStream

object Serializer {
  val defaultSerializerName = classOf[JavaSerializer].getName
  type Identifier = Byte
}

/**
 * A Serializer represents a bimap between an object and an array of bytes representing that object
 */
trait Serializer extends scala.Serializable {
  /**
   * Completely unique Byte value to identify this implementation of Serializer, used to optimize network traffic
   * Values from 0 to 16 is reserved for Akka internal usage
   */
  def identifier: Serializer.Identifier

  /**
   * Serializes the given object into an Array of Byte
   */
  def toBinary(o: AnyRef): Array[Byte]

  /**
   *  Produces an object from an array of bytes, with an optional type-hint and a classloader to load the class into
   */
  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]] = None, classLoader: Option[ClassLoader] = None): AnyRef
}

object JavaSerializer extends JavaSerializer
object NullSerializer extends NullSerializer

class JavaSerializer extends Serializer {

  def identifier = 1: Byte

  def toBinary(o: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    out.writeObject(o)
    out.close()
    bos.toByteArray
  }

  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]] = None,
                 classLoader: Option[ClassLoader] = None): AnyRef = {
    val in =
      if (classLoader.isDefined) new ClassLoaderObjectInputStream(classLoader.get, new ByteArrayInputStream(bytes)) else
        new ObjectInputStream(new ByteArrayInputStream(bytes))
    val obj = in.readObject
    in.close()
    obj
  }
}

class NullSerializer extends Serializer {

  val nullAsBytes = Array[Byte]()

  def identifier = 0: Byte
  def toBinary(o: AnyRef) = nullAsBytes
  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]] = None, classLoader: Option[ClassLoader] = None): AnyRef = null
}
