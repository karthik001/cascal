package com.shorrockin.cascal.model

import java.nio.ByteBuffer
import org.apache.cassandra.thrift.{ColumnOrSuperColumn}

/**
 * implementation of a standard key, which is an object which can be thought
 * of as a container for a list of columns. The parent of this will either
 * be a StandardColumnFamily or a SuperKey
 *
 * @author Chris Shorrock
 */
case class StandardKey(value:ByteBuffer, family:StandardColumnFamily) 
		extends Key[Column, Seq[Column]]
    with StandardColumnContainer[Column, Seq[Column]] {

  def \(name:ByteBuffer) = new Column(name=name, value=null,owner=this)
  def \(name:ByteBuffer, value:ByteBuffer) = new Column(name=name, value=value, owner=this)
  def \(name:ByteBuffer, value:ByteBuffer, time:Long) = new Column(name=name, value=value, time=time, owner=this)

  def convertListResult(results:Seq[ColumnOrSuperColumn]):Seq[Column] = {
    results.map { (result) =>
      val column = result.getColumn
      \(ByteBuffer.wrap(column.getName), ByteBuffer.wrap(column.getValue), column.getTimestamp)
    }
  }

  override def toString = "%s \\ StandardKey(value = '%s')".format(family.toString, value)
}