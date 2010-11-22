package com.shorrockin.cascal.model

import java.nio.ByteBuffer

/**
 * abstraction for the standard column family. a standard column family
 * contains a collection of keys each mapped to a collection of columns.
 *
 * @author Chris Shorrock
 */
case class StandardColumnFamily(value:String, keyspace:Keyspace)
        extends ColumnFamily[StandardKey] {
  def \(v:ByteBuffer) = new StandardKey(v, this)
  override def toString = "%s \\ StandardColumnFamily(value = %s)".format(keyspace.toString, value)
}