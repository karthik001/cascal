package com.shorrockin.cascal

import testing._
import model.Column
import org.junit.{Assert, Test}
import session.{ColumnPredicate, Delete, Insert}
import utils.{UUID, Conversions}

class TestBatchOperations extends CassandraTestPool {
  import Conversions._
  import Assert._

  @Test def testStandardBatchInsertion = borrow { (s) =>
    val key = "Test" \ "Standard" \ UUID()
    val col1 = key \ ("Column-a-1", "Value-1")
    val col2 = key \ ("Column-a-2", "Value-2")
    val col3 = key \ ("Column-a-3", "Value-3")

    val list1 = Insert(col1) :: Insert(col2) :: Insert(col3)
    s.batch(list1)
//    s.insert(col1)
//    s.insert(col2)
//    s.insert(col3)
//
    println("owner: %s".format(col1.owner))
    val list = s.list(key)
    assertEquals(3, list.size)
  }

  @Test def testSuperBatchInsertion = borrow { (s) =>
    val key1 = "Test" \\ "Super" \ UUID()
    val key2 = "Test" \\ "Super" \ UUID()
    val sc1  = key1 \ UUID()
    val sc12 = key1 \ UUID()
    val sc2  = key2 \ UUID()
    val col1 = sc1 \ ("Col1", "Val1")
    val col2 = sc1 \ ("Col2", "Val2")
    val col3 = sc12 \ ("Col3", "Val3")
    val col4 = sc2 \ ("Col4", "Val4")

    s.batch(Insert(col1) :: Insert(col2) :: Insert(col3) :: Insert(col4))

    assertEquals(2, s.list(sc1).size)
    assertEquals(1, s.list(sc12).size)
    assertEquals(1, s.list(sc2).size)

    val superCols = s.list(key1)
    val combined = superCols.foldLeft(List[Column]()) { (left, right) => right._2.toList ::: left}
    assertEquals(2, superCols.size)
    assertEquals(3, combined.toList.size)
  }

  @Test def testStandardBatchDelete = borrow { (s) =>
    val key  = "Test" \ "Standard" \ UUID()
    val col1 = key \ ("Column-b-1", "Value-1")
    val col2 = key \ ("Column-b-2", "Value-2")
    val col3 = key \ ("Column-b-3", "Value-3")
    val col4 = key \ ("Column-b-4", "Value-4")

    s.batch(Insert(col1) :: Insert(col2) :: Insert(col3))
    assertEquals(3, s.list(key).size)

    s.batch(Delete(key, ColumnPredicate(col2.name :: col3.name :: Nil)) :: Insert(col4))

    assertEquals(2, s.list(key).size)
    assertEquals(None, s.get(col2))
    assertEquals(None, s.get(col3))
    assertEquals("Value-1", string(s.get(col1).get.value))
    assertEquals("Value-4", string(s.get(col4).get.value))
  }

  @Test def testBatchSuperDelete = borrow { (s) =>
    val key1 = "Test" \\ "Super" \ UUID()
    val sc1  = key1 \ UUID()
    val sc2  = key1 \ UUID()
    val col1 = sc1  \ ("Col1", "Val1")
    val col2 = sc1  \ ("Col2", "Val2")
    val col3 = sc2  \ ("Col3", "Val3")

    s.batch(Insert(col1) :: Insert(col2) :: Insert(col3))

    assertEquals(2, s.list(key1).size)
    assertEquals(2, s.list(sc1).size)
    assertEquals(1, s.list(sc2).size)

    s.batch(Delete(sc2) :: Nil)
    assertEquals(0, s.list(sc2).size)

    /* TODO wierdness within
    s.batch(Delete(key1, ColumnPredicate(col1.name :: col2.name :: Nil)) :: Nil)

    assertEquals(0, s.list(sc1).size)
    assertEquals(0, s.list(key1).size)
    */
  }
}
