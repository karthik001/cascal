package com.shorrockin.cascal.model

import java.nio.ByteBuffer

/**
 * a column family which houses super columns
 *
 * @author Chris Shorrock
 */
case class SuperColumnFamily(value:String, keyspace:Keyspace) extends ColumnFamily[SuperKey] {
  def \(v:ByteBuffer) = new SuperKey(v, this)
  override def toString = "%s \\\\ SuperColumnFamily(value = %s)".format(keyspace.toString, value)
}