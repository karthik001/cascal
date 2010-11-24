package com.shorrockin.cascal.model

import java.util.Date
import java.nio.ByteBuffer

import org.apache.cassandra.thrift.{ColumnPath, ColumnOrSuperColumn, Column => CassColumn}
import org.apache.cassandra.thrift.{ColumnParent, SuperColumn => CassSuperColumn}

import com.shorrockin.cascal.utils.Conversions
import com.shorrockin.cascal.utils.Utils.now


/**
 * a column is the child component of a super column or a
 * standard key.
 *
 * @author Chris Shorrock
 * @param Owner the type of object which owns this column
 */
case class Column(name: ByteBuffer, value: ByteBuffer, time: Long = now, owner: ColumnContainer[_, _])
        extends Gettable[Column] {
  val partial = (value == null)
  val key = owner.key
  val family = key.family
  val keyspace = key.keyspace


  // columnParent
  lazy val columnParent = owner match {
    case sup: SuperColumn => new ColumnParent(family.value).setSuper_column(sup.value)
    case std: StandardKey => new ColumnParent(family.value)
  }

  // thrift.Column
  lazy val cassandraColumn = new CassColumn(name, value, time)

  lazy val columnPath = owner match {
    case sup: SuperColumn => new ColumnPath(family.value).setColumn(name).setSuper_column(sup.value)
    case std: StandardKey => new ColumnPath(family.value).setColumn(name)
  }


  lazy val columnOrSuperColumn = owner match {
    case std: StandardKey =>
      new ColumnOrSuperColumn().setColumn(new CassColumn(name, value, time))
    case sup: SuperColumn =>
      val list = Conversions.toJavaList(new CassColumn(name, value, time) :: Nil)
      new ColumnOrSuperColumn().setSuper_column(new CassSuperColumn(sup.value, list))
  }



  /**
   * copy method to create a new instance of this column with a new value and
   * the same other values.
   */
  def \(newValue: ByteBuffer) = new Column(name, newValue, time, owner)


  /**
   * appends a column onto this one forming a list
   */
  def ::(other: Column): List[Column] = other :: this :: Nil

  /**
   * given the cassandra object returned from retrieving this object,
   * returns an instance of our return type.
   */
  def convertGetResult(colOrSuperCol: ColumnOrSuperColumn): Column = {
    val col = colOrSuperCol.getColumn
    Column(ByteBuffer.wrap(col.getName), ByteBuffer.wrap(col.getValue), col.getTimestamp, owner)
  }

  private def stringIfPossible(a: ByteBuffer): String = {
    if (a.array.length <= 4) return "Array (" + a.array.mkString(", ") + ")"
    if (a.array.length > 1000) return a.toString
    try {Conversions.string(a)} catch {case _ => a.toString}
  }

  override def toString = "%s \\ Column(name = '%s', value = '%s', time = '%s')".
          format(owner.toString, stringIfPossible(name), stringIfPossible(value), time)
}