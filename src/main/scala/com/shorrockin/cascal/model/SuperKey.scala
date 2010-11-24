package com.shorrockin.cascal.model

import java.nio.ByteBuffer
import org.apache.cassandra.thrift.{ColumnParent, ColumnPath, ColumnOrSuperColumn}

/**
 *@author Chris Shorrock, Michael Fortin
 */
case class SuperKey(value:ByteBuffer, family:SuperColumnFamily)
		extends Key[SuperColumn, Seq[(SuperColumn, Seq[Column])]] {

  def \(v:ByteBuffer) = new SuperColumn(v, this)
  override lazy val columnPath = new ColumnPath(family.value).setSuper_column(value)
  override lazy val columnParent = new ColumnParent(family.value)

  /**
   *  converts a list of super columns to the specified return type
   */
  def convertListResult(results:Seq[ColumnOrSuperColumn]):Seq[(SuperColumn, Seq[Column])] = {
    results.map { (result) =>
      val nativeSuperCol = result.getSuper_column
      val superColumn    = this \ ByteBuffer.wrap(nativeSuperCol.getName)
      val columns = convertList(nativeSuperCol.getColumns).map { (column) =>
        superColumn \ (ByteBuffer.wrap(column.getName), ByteBuffer.wrap{column.getValue}, column.getTimestamp)
      }
      (superColumn -> columns)
    }
  }

  private def convertList[T](v:java.util.List[T]):List[T] = {
	 scala.collection.JavaConversions.asBuffer(v).toList
  }

  override def toString = "%s \\ SuperKey(value = %s)".format(family.toString, value)
}