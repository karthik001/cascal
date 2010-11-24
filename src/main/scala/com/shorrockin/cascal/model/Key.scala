package com.shorrockin.cascal.model

import org.apache.cassandra.thrift.{ColumnParent, ColumnPath}
import java.nio.ByteBuffer

/**
 * a key is an abstract type which maps to either a standard key which contains
 * a collection of columns, or a super key, which contains a collection of
 * standard keys.
 *
 * @author Chris Shorrock
 * @param ColumnType the type of column that this key holds
 * @param ListType when this key is used in a list the type of object that is returned
 */
trait Key[C, T] extends ByteValue with ColumnContainer[C, T] {
  val value:ByteBuffer

  val keyspace = family.keyspace
  val key = this
  lazy val columnPath = new ColumnPath().setColumn_family(family.value)
  lazy val columnParent = new ColumnParent().setColumn_family(family.value)

  def ::(other:Key[C, T]):List[Key[C, T]] = other :: this :: Nil
}