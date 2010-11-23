package com.shorrockin.cascal

import serialization.annotations._
import serialization.Converter
import testing._
import org.junit.{Assert, Test}
import utils.{UUID, Conversions}
import java.util.{UUID => JavaUUID, Date}

/**
 * tests the UUID ability to convert to and from bytes, strings, etc.
 */
class TestUUIDByteConversion extends CassandraTestPool {
  import Assert._
  import Conversions._

  @Test def ensureUUIDConvertsToFromBytes = {
    val original = UUID()
    assertEquals(16,Conversions.bytes(original).array.length)
    assertEquals(original, UUID(Conversions.bytes(original).array))
  }

  @Test def ensureUUIDConvertsToStringAndBack = {
    val original = UUID()
    val string   = Conversions.string(original)
    assertEquals(original, Conversions.uuid(string))
  }

  @Test def testUUIDColumnMapping = borrow { session =>
    val uuid = UUID()
    val toInsert = "Test" \\ "Super" \ "UUIDColumnMapping-test" \ uuid \ "Column" \ "Value"
    
    session.insert(toInsert)
    val results = session.list("Test" \\ "Super" \ "UUIDColumnMapping-test" )
    println("### results: %s".format(results))
    
    val result = results.map(c=>Converter[MappedUUID](c._2))

    assertEquals(uuid, result(0).s)
    assertEquals("Value", result(0).b)
  }
}

@Keyspace("Test") @Family("Super") @Super
case class MappedUUID(@Key a:String, @SuperColumn s:JavaUUID, @Value("Column") b:String)
