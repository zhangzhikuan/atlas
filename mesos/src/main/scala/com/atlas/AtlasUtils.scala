package com.atlas

import java.io._

import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object AtlasUtils {
  private val LOG = LoggerFactory.getLogger("Utils")

  /**
    * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
    * exceptions as IOException. This is used when implementing Externalizable and Serializable's
    * read and write methods, since Java's serializer will not report non-IOExceptions properly;
    * see SPARK-4080 for more context.
    */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        LOG.error("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        LOG.error("Exception encountered", e)
        throw new IOException(e)
    }
  }

  /** Executes the given block. Log non-fatal errors if any, and only throw fatal errors */
  def tryLogNonFatalError(block: => Unit) {
    try {
      block
    } catch {
      case NonFatal(t) =>
        LOG.error(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
    }
  }

  /** Serialize an object using Java serialization */
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /** Deserialize an object using Java serialization */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

  /** Deserialize an object using Java serialization and the given ClassLoader */
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] = {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      }
    }
    ois.readObject.asInstanceOf[T]
  }

  def deserializeLongValue(bytes: Array[Byte]): Long = {
    // Note: we assume that we are given a Long value encoded in network (big-endian) byte order
    var result = bytes(7) & 0xFFL
    result = result + ((bytes(6) & 0xFFL) << 8)
    result = result + ((bytes(5) & 0xFFL) << 16)
    result = result + ((bytes(4) & 0xFFL) << 24)
    result = result + ((bytes(3) & 0xFFL) << 32)
    result = result + ((bytes(2) & 0xFFL) << 40)
    result = result + ((bytes(1) & 0xFFL) << 48)
    result + ((bytes(0) & 0xFFL) << 56)
  }
  def closable[A <: {def close() : Unit}, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      if (resource != null)
        resource.close()
    }
}
