package com.shorrockin.cascal.model

import org.apache.cassandra.thrift.{ColumnParent, ColumnPath, ColumnOrSuperColumn}
import java.nio.ByteBuffer

/**
 * defines a cassandra object which houses a collection of columns. Generally
 * this will be either a StandardKey, a SuperKey, or a SuperColumn. As each of
 * these house columns, with the SuperKey housing SuperColumns and the other two
 * housing DepStandardColumn.
 *
 * @author Chris Shorrock
 * @param ColumnType the type of columns that this container houses.
 * @param ListType when listed, what type of object does it return.
 */
trait ColumnContainer[C, L] extends ByteValue {
  
  def \(value:ByteBuffer):C

  val family:ColumnFamily[_]
  val key:Key[_, _]
  val keyspace:Keyspace
  val columnPath:ColumnPath
  val columnParent:ColumnParent

  def convertListResult(results:Seq[ColumnOrSuperColumn]):L

}
