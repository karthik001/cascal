package com.shorrockin.cascal

import java.util.Date
import serialization.annotations._
import serialization.{LongSerializer, DateSerializer, Converter}
import utils.Conversions
import org.junit.{Assert, Test}

/**
 *
 */
class TestSerialization {
  import Conversions._
  import Assert._

  @Test def testCanConvertFromColumnsToMappedStandard() {
    val now  = new Date
    val key  = "Test" \ "Standard" \ 876L
    val colb = key \ "Column-B" \ now
    val colc = key \ "Column-C" \ 12L
    val cols = colc :: colb

    val obj = Converter[MappedStandard](cols)
    assertEquals(876L, obj.a)
    assertEquals(now, obj.b)
    assertEquals(12L, obj.c)
  }


  @Test def testCanConvertDynamicMapValue() {
    val now  = new Date
    val key  = "Test" \ "Standard" \ 12345L
    val colb = key \ "Column-B" \ 123
    val colc = key \ "Column-C" \ 12
    val cols = colc :: colb

    val obj = Converter[DynamicMappedStandard](cols)
    assertEquals(12345L, obj.key)
    assertEquals(2, obj.values.size)
    assertEquals("Column-C", obj.values(0)._1)
    assertEquals(123, obj.values(1)._2)
  }

  @Test def testCanConvertOptionColumnsToMappedStandard() {
    val key     = "Test" \ "Standard" \ "Hello"
    val valid   = key \ "Column" \ 12L
    val inValid = key \ "XYZ" \ 12

    val someObj = Converter[MappedOptionStandard](valid :: Nil)
    val noneObj = Converter[MappedOptionStandard](inValid :: Nil)

    assertTrue(someObj.value.isDefined)
    assertTrue(noneObj.value.isEmpty)
    assertEquals(12L, someObj.value.get)
  }


  @Test def testCanConvertFromMapToSeqObjects() {
    val key = "Test" \\ "Super" \ "Key"
    val col1 = (key \ "SC1" \ "C" \ "Foo") :: Nil
    val col2 = (key \ "SC2" \ "C" \ "Bar") :: Nil
    val seq  = List((col1(0).owner -> col1), (col2(0).owner -> col2))

    val objects = seq.map(tup=>Converter[MappedOptionSuper](tup._2))

    assertEquals(2, objects.length)
    assertEquals("Foo", objects(0).value.get)
    assertEquals("Bar", objects(1).value.get)
  }

  @Test def testCanConvertFromColumnsToMappedSuper() {
    val now  = new Date
    val key  = "Test" \\ "Super" \ "Hello"
    val sc   = key \ "Super Column Value"
    val colb = sc \ "Column-B" \ now
    val colc = sc \ "Column-C" \ 12L
    val cold = sc \ "Column-D" \ 13L

    val obj = Converter[MappedSuper](colc :: colb)
    assertEquals("Hello", obj.a)
    assertEquals("Super Column Value", obj.s)
    assertEquals(now, obj.b)
    assertEquals(12L, obj.c)    

    val obj2 = Converter[MappedSuperWithCols](colc :: cold)
    assertEquals("Hello", obj.a)
    assertEquals("Super Column Value", obj.s)
    assertTrue(obj2.values.contains("Column-C"));
    assertTrue(obj2.values.contains("Column-D"));

  }

  @Test def testCanConvertObjectToStandardColumnList = {}

  @Test def testCanConvertObjectToSuperColumnList() = {}
}



@Keyspace("Test") @Family("Standard")
case class MappedStandard(@Key a:Long, @Value("Column-B") b:Date, @Value("Column-C") c:Long)

@Keyspace("Test") @Family("Standard")
case class DynamicMappedStandard(@Key key:Long, @Columns(name=classOf[String], value=classOf[Int]) values:Seq[(String, Int)])

@Keyspace("Test") @Family("Super") @Super
case class MappedSuper(@Key a:String, @SuperColumn s:String, @Value("Column-B") b:Date, @Value("Column-C") c:Long)

@Keyspace("Test") @Family("Super") @Super
case class MappedSuperWithCols(@Key a:String, @SuperColumn s:String, @Columns(name=classOf[String], value=classOf[Long]) raw:Seq[(String, Long)]) {
  val values = raw.map { _._1 }
}

@Keyspace("Test") @Family("Standard")
case class MappedOptionStandard(@Optional(column="Column", as=classOf[Long]) value:Option[Long])

@Keyspace("Test") @Family("Super") @Super
case class MappedOptionSuper(@Optional(column="C", as=classOf[String]) value:Option[String])

