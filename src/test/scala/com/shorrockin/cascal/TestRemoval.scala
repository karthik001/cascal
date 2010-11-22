package com.shorrockin.cascal

import testing._
import org.junit.{Assert, Test}
import com.shorrockin.cascal.utils.UUID
import java.nio.ByteBuffer

/**
 * tests session removal
 */
class TestRemoval extends CassandraTestPool {
  import com.shorrockin.cascal.utils.Conversions._
  import Assert._
  val superKeyBuf = ByteBuffer.allocate(16).put("SuperKey".getBytes)

  @Test def testKeyRemoval = borrow { (s) =>
    val std = s.insert("Test" \ "Standard" \ UUID() \ ("Column", "Value"))

    val sup = s.insert("Test" \\ "Super" \ superKeyBuf \ UUID() \ ("Column", "Value"))

    s.remove(std.key)
    s.remove(sup.key)

    assertEquals(None, s.get(std))
    assertEquals(None, s.get(sup))
  }

  @Test def testColumnRemoval = borrow { (s) =>
    val std = s.insert("Test" \ "Standard" \ UUID() \ ("Column", "Value"))
    val sup = s.insert("Test" \\ "Super" \ superKeyBuf \ UUID() \ ("Column", "Value"))

    s.remove(std)
    s.remove(sup)

    assertEquals(None, s.get(std))
    assertEquals(None, s.get(sup))
  }
}
