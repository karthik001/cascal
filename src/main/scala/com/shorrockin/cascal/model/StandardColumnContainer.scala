package com.shorrockin.cascal.model

import java.nio.ByteBuffer

/**
 * a type of column container which holds standard columns.
 *
 * @author Chris Shorrock
 */
trait StandardColumnContainer[C, S] extends ColumnContainer[C, S] {
  def \(name:ByteBuffer):C
  def \(name:ByteBuffer, value:ByteBuffer):C
  def \(name:ByteBuffer, value:ByteBuffer, time:Long):C  
}
