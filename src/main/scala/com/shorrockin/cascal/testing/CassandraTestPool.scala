package com.shorrockin.cascal.testing

import org.apache.cassandra.thrift.CassandraDaemon
import java.io.File

import org.apache.thrift.transport.TSocket
import com.shorrockin.cascal.session._
import com.shorrockin.cascal.utils.{Utils, Logging}
import org.apache.cassandra.config.{KSMetaData, CFMetaData, DatabaseDescriptor}

/**
 * trait which mixes in the functionality necessary to embed
 * cassandra into a unit test
 */
trait CassandraTestPool extends Logging {
  def borrow(f: (Session) => Unit) = {
    EmbeddedTestCassandra.init
    EmbeddedTestCassandra.pool.borrow(f)
  }
}

/**
 * maintains the single instance of the cassandra server
 */
object EmbeddedTestCassandra extends Logging {
  import Utils._
  var initialized = false

  val hosts = Host("localhost", 9160, 250) :: Nil
  val params = new PoolParams(10, ExhaustionPolicy.Fail, 500L, 6, 2)
  lazy val pool = new SessionPool("Test", hosts, params, Consistency.One)

  /**
   * For cassandra 0.7, you need to explicitly load the schema.  This was copied from
   * org.apache.cassandra.SchemaLoader
   */
  def loadSchemaFromYaml = {
    import collection.JavaConversions._
    
    for (ksm: KSMetaData <- DatabaseDescriptor.readTablesFromYaml()) {
      for (cfm: CFMetaData <- ksm.cfMetaData().values())
        CFMetaData.map(cfm)
      DatabaseDescriptor.setTableDefinition(ksm, DatabaseDescriptor.getDefsVersion())
    }
  }

  def init = synchronized {
    if (!initialized) {
      val homeDirectory = new File("target/cassandra.home.unit-tests")
      //      delete(homeDirectory)
      //      homeDirectory.mkdirs

      log.debug("creating cassandra instance at: " + homeDirectory.getCanonicalPath)
      //      log.debug("copying cassandra configuration files to root directory")

      val fileSep = System.getProperty("file.separator")
      // val storageFile = new File(homeDirectory, "storage-conf.xml")
      //      val storageFile = new File(homeDirectory, "cassandra.yaml")
      //      val logFile     = new File(homeDirectory, "log4j.properties")
      //
      //      replace(copy(resource("/cassandra.yaml"), storageFile), ("%temp-dir%" -> (homeDirectory.getCanonicalPath + fileSep)))
      //      copy(resource("/log4j.properties"), logFile)

      loadSchemaFromYaml
      System.setProperty("storage-config", homeDirectory.getCanonicalPath)
      log.debug("creating data file and log location directories")
      DatabaseDescriptor.getAllDataFileLocations.foreach {(file) => new File(file).mkdirs}
      // new File(DatabaseDescriptor.getLogFileLocation).mkdirs

      val daemon = new CassandraDaemonThread
      daemon.start

      // try to make sockets until the server opens up - there has to be a better
      // way - just not sure what it is.
      Thread.sleep(3000)
      log.debug("Sleep for 3s")

      val socket = new TSocket("localhost", 9160)
      var opened = false
      while (!opened) {
        try {
          socket.open()
          opened = true
          log.debug("I was able to make a connection")
        }
        catch {
          case e: Throwable => log.error("******************** Not started", e)
        }
        finally {
          socket.close()
        }
      }

      initialized = true
    }
  }

  private def resource(str: String) = classOf[CassandraTestPool].getResourceAsStream(str)
}

/**
 * daemon thread used to start and stop cassandra
 */
class CassandraDaemonThread extends Thread("CassandraDaemonThread") with Logging {
  private val daemon = new CassandraDaemon

  setDaemon(true)

  /**
   * starts the server and blocks until it has
   * completed booting up.
   */
  def startServer = {

  }

  override def run: Unit = {
    log.debug("initializing cassandra daemon")
    daemon.init(new Array[String](0))
    log.debug("starting cassandra daemon")
    daemon.start
  }

  def close(): Unit = {
    log.debug("instructing cassandra deamon to shut down")
    daemon.stop
    log.debug("blocking on cassandra shutdown")
    this.join
    log.debug("cassandra shut down")
  }
}

