package com.shorrockin.cascal.session

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift.{Mutation, Cassandra, NotFoundException, ConsistencyLevel}
import org.apache.thrift.transport.{TFramedTransport, TSocket}

import java.util.{Map => JMap, List => JList, HashMap, ArrayList}
import java.nio.ByteBuffer

import com.shorrockin.cascal.utils.Conversions._
import com.shorrockin.cascal.utils.Utils.now
import com.shorrockin.cascal.model._

import collection.JavaConversions._
import collection.mutable.ListBuffer


/**
 * a cascal session is the entry point for interacting with the
 * cassandra system through various path elements.
 *
 * @author Chris Shorrock
 */
class Session(val keySpace: String,
              val host: Host = Host("localhost", 9160, 250),
              val defaultConsistency: Consistency = Consistency.One,
              val framedTransport: Boolean = false)
        extends SessionTemplate {
  // TODO sessions need to be lazily initialized

  private val sock = new TFramedTransport(new TSocket(host.address, host.port, host.timeout))

  private val protocol = new TBinaryProtocol(sock)

  var lastError: Option[Throwable] = None

  val client = new Cassandra.Client(protocol, protocol)

  /**
   * opens the socket
   */
  def open() = {
    sock.open()
    client.set_keyspace(keySpace)
  }


  /**
   * closes this session
   */
  def close() = {
    sock.close()
    protocol.getTransport.close()
  }


  /**
   * returns true if this session is open
   */
  def isOpen = sock.isOpen


  /**
   * true if the we experienced an error while using this session
   */
  def hasError = lastError.isDefined


  /**
   * return the current cluster name of the cassandra instance
   */
  lazy val clusterName = client.describe_cluster_name()

  /**
   * returns the version of the cassandra instance
   */
  lazy val version = client.describe_version()


  /**
   * returns all the keyspaces from the cassandra instance
   */
  lazy val keyspaces: Seq[String] = client.describe_keyspaces.map(_.name)

  /**
   * returns the descriptors for all keyspaces
   */
  lazy val keyspaceDescriptors: List[Tuple3[String, String, String]] = {
    // TODO Replace this with the descriptors of just this keyspace
    val keyspaceDesc = new ListBuffer[Tuple3[String, String, String]]
    val describekeyspaces = client.describe_keyspaces
    describekeyspaces.foreach {
      space =>
        val list = space.cf_defs
        list.foreach {
          family =>
            keyspaceDesc += ((space.name, family.name, family.column_type))
        }
    }
    keyspaceDesc.toList
  }

  def verifyInsert[E](col: Column[E]) {
    var famType = if (col.owner.isInstanceOf[SuperColumn]) "Super" else "Standard"
    if (!keyspaceDescriptors.contains(col.keyspace.value, col.family.value, famType)) {
      throw new IllegalArgumentException("Keyspace %s or ColumnFamily %s of type %s does not exist in this cassandra instance".format(col.keyspace.value, col.family.value, famType))
    }
  }

  def verifyRemove(container: ColumnContainer[_, _]) {
    if (!keyspaceDescriptors.contains(container.keyspace.value, container.family.value, "Standard") &&
            !keyspaceDescriptors.contains(container.keyspace.value, container.family.value, "Super"))
      throw new IllegalArgumentException("Keyspace %s or ColumnFamily %s does not exist in this cassandra instance".format(container.keyspace.value, container.family.value))
  }

  def verifyOperation(op: Operation) {
    if (op.isInstanceOf[Insert]) {
      verifyInsert(op.asInstanceOf[Insert].column)
    } else if (op.isInstanceOf[Delete]) {
      verifyRemove(op.asInstanceOf[Delete].container)
    }
  }


  /**
   *  returns the column value for the specified column
   */
  def get[ResultType](col: Gettable[ResultType], consistency: Consistency): Option[ResultType] = detect {
    try {
      // (x$1: java.nio.ByteBuffer,x$2: org.apache.cassandra.thrift.ColumnPath,x$3: org.apache.cassandra.thrift.ConsistencyLevel)
      val result = client.get(col.family.value, col.columnPath, consistency)
      Some(col.convertGetResult(result))
    } catch {
      case nfe: NotFoundException => None
    }
  }


  /**
   * returns the column value for the specified column, using the default consistency
   */
  def get[ResultType](col: Gettable[ResultType]): Option[ResultType] = get(col, defaultConsistency)


  /**
   * inserts the specified column value
   */
  def insert[E](col: Column[E], consistency: Consistency) = detect {
    verifyInsert(col)
    // (x$1: java.nio.ByteBuffer,x$2: org.apache.cassandra.thrift.ColumnParent,x$3: org.apache.cassandra.thrift.Column,x$4: org.apache.cassandra.thrift.ConsistencyLevel)
    println(col)
    val family = col.family.value
    val parent = col.columnParent
    val column = col.cassandraColumn
    println("*** insert: %s // %s / %s".format(family,parent,column))
    client.insert(family, parent, column, consistency)
    col
  }


  /**
   * inserts the specified column value using the default consistency
   */
  def insert[E](col: Column[E]): Column[E] = insert(col, defaultConsistency)


  /**
   *   counts the number of columns in the specified column container
   */
  def count(container: ColumnContainer[_, _], predicate: Predicate, consistency: Consistency): Int = detect {
    // org.apache.cassandra.thrift.ColumnParent
    client.get_count(container.family.value, container.columnParent, predicate.slicePredicate, consistency)
  }


  /**
   * performs count on the specified column container
   */
  def count(container: ColumnContainer[_, _], predicate: Predicate): Int =
    count(container, predicate, defaultConsistency)

  def count(container: ColumnContainer[_, _]): Int =
    count(container, RangePredicate("", ""), defaultConsistency)


  /**
   * removes the specified column container
   */
  def remove(container: ColumnContainer[_, _], consistency: Consistency): Unit = detect {
    verifyRemove(container)
    //  (x$1: java.nio.ByteBuffer,x$2: org.apache.cassandra.thrift.ColumnPath,x$3: Long,x$4: org.apache.cassandra.thrift.ConsistencyLevel)
    client.remove(container.family.value, container.columnPath, now, consistency)
  }


  /**
   * removes the specified column container using the default consistency
   */
  def remove(container: ColumnContainer[_, _]): Unit = remove(container, defaultConsistency)


  /**
   * removes the specified column container
   */
  def remove(column: Column[_], consistency: Consistency): Unit = detect {
    // (x$1: java.nio.ByteBuffer,x$2: org.apache.cassandra.thrift.ColumnPath,x$3: Long,x$4: org.apache.cassandra.thrift.ConsistencyLevel)
    client.remove(column.family.value, column.columnPath, now, consistency)
  }


  /**
   * removes the specified column container using the default consistency
   */
  def remove(column: Column[_]): Unit = remove(column, defaultConsistency)


  /**
   * performs a list of the provided standard key. uses the list of columns as the predicate
   * to determine which columns to return.
   */
  def list[ResultType](container: ColumnContainer[_, ResultType], predicate: Predicate, consistency: Consistency): ResultType = detect {
    // (x$1: java.nio.ByteBuffer,x$2: org.apache.cassandra.thrift.ColumnParent,x$3: org.apache.cassandra.thrift.SlicePredicate,x$4: org.apache.cassandra.thrift.ConsistencyLevel)

    val results = client.get_slice(container.family.value, container.columnParent, predicate.slicePredicate, consistency)
    container.convertListResult(results)
  }


  /**
   * performs a list of the specified container using no predicate and the default consistency.
   */
  def list[ResultType](container: ColumnContainer[_, ResultType]): ResultType = list(container, EmptyPredicate, defaultConsistency)


  /**
   * performs a list of the specified container using the specified predicate value
   */
  def list[ResultType](container: ColumnContainer[_, ResultType], predicate: Predicate): ResultType =
    list(container, predicate, defaultConsistency)


  /**
   * given a list of containers, will perform a slice retrieving all the columns specified
   * applying the provided predicate against those keys. Assumes that all the containers
   * generate the same columnParent. That is, if they are super columns, they all have the
   * same super column name (existing to separate key values), and regardless of column
   * container type - belong to the same column family. If they are not, the first key
   * in the sequence what is used in this query.<br>
   * NOTE (to be clear): If containers is a 
   */
  def list[ColumnType, ResultType](containers: Seq[ColumnContainer[ColumnType, ResultType]], predicate: Predicate, consistency: Consistency): Seq[(ColumnContainer[ColumnType, ResultType], ResultType)] = {
    if (containers.size > 0) detect {
      val firstContainer = containers(0)
      val keyspace = firstContainer.keyspace
      val keyStrings = containers.map {k => ByteBuffer.wrap(k.key.value.getBytes)}.asInstanceOf[Seq[ByteBuffer]]


      // (x$1: java.util.List[java.nio.ByteBuffer],x$2: org.apache.cassandra.thrift.ColumnParent,x$3: org.apache.cassandra.thrift.SlicePredicate,x$4: org.apache.cassandra.thrift.ConsistencyLevel)
      val results = client.multiget_slice(toJavaList(keyStrings), firstContainer.columnParent, predicate.slicePredicate, consistency)

      def locate(key: String) = (containers.find {_.key.value.equals(key)}).get

      results.map {
        (tuple) =>
          val key = locate(tuple._1)
          val value = key.convertListResult(tuple._2)
          (key -> value)
      }.toSeq
    } else {
      throw new IllegalArgumentException("must provide at least 1 container for a list(keys, predicate, consistency) call")
    }
  }


  /**
   * @see list ( Seq[ColumnContainer], Predicate, Consistency )
   */
  def list[ColumnType, ResultType](containers: Seq[ColumnContainer[ColumnType, ResultType]]): Seq[(ColumnContainer[ColumnType, ResultType], ResultType)] = list(containers, EmptyPredicate, defaultConsistency)


  /**
   * @see list ( Seq[ColumnContainer], Predicate, Consistency )
   */
  def list[ColumnType, ResultType](containers: Seq[ColumnContainer[ColumnType, ResultType]], predicate: Predicate): Seq[(ColumnContainer[ColumnType, ResultType], ResultType)] = list(containers, predicate, defaultConsistency)


  /**
   * performs a list on a key range in the specified column family. the predicate is applied
   * with the provided consistency guaranteed. the key range may be a range of keys or a range
   * of tokens. This list call is only available when using an order-preserving partition.
   */
  def list[ColumnType, ListType](family: ColumnFamily[Key[ColumnType, ListType]], range: KeyRange, predicate: Predicate, consistency: Consistency): Map[Key[ColumnType, ListType], ListType] = detect {
    // (x$1: org.apache.cassandra.thrift.ColumnParent,x$2: org.apache.cassandra.thrift.SlicePredicate,x$3: org.apache.cassandra.thrift.KeyRange,x$4: org.apache.cassandra.thrift.ConsistencyLevel)
    val results = client.get_range_slices(family.columnParent, predicate.slicePredicate, range.cassandraRange, consistency)
    var map = Map[Key[ColumnType, ListType], ListType]()

    results.foreach {
      (keyslice) =>
        val key = (family \ keyslice.key)
        map = map + (key -> key.convertListResult(keyslice.columns))
    }
    map
  }


  /**
   * performs a key-range list without any predicate
   */
  def list[ColumnType, ListType](family: ColumnFamily[Key[ColumnType, ListType]], range: KeyRange, consistency: Consistency): Map[Key[ColumnType, ListType], ListType] = {
    list(family, range, EmptyPredicate, consistency)
  }


  /**
   * performs a key-range list without any predicate, and using the default consistency
   */
  def list[ColumnType, ListType](family: ColumnFamily[Key[ColumnType, ListType]], range: KeyRange): Map[Key[ColumnType, ListType], ListType] = {
    list(family, range, EmptyPredicate, defaultConsistency)
  }


  /**
   * performs the specified seq of operations in batch. assumes all operations belong
   * to the same keyspace. If they do not then the first keyspace in the first operation
   * is used.
   */
  def batch(ops: Seq[Operation], consistency: Consistency): Unit = {
    if (ops.size > 0) detect {
      val keyToFamilyMutations = new HashMap[ByteBuffer, JMap[String, JList[Mutation]]]()
//      val keyspace = ops(0).keyspace

      def getOrElse[A, B](map: JMap[A, B], key: A, f: => B): B = {
        if (map.containsKey(key)) {
          map.get(key)
        } else {
          val newValue = f
          map.put(key, newValue)
          newValue
        }
      }

      ops.foreach { op =>
        verifyOperation(op)
        val familyToMutations = getOrElse(keyToFamilyMutations, op.key.value, new HashMap[String, JList[Mutation]]())
        val mutationList = getOrElse(familyToMutations, op.family.value, new ArrayList[Mutation]())
        mutationList.add(op.mutation)
      }

      // TODO may need to flatten duplicate super columns?

      // (x$1: java.util.Map[java.nio.ByteBuffer,java.util.Map[java.lang.String,java.util.List[org.apache.cassandra.thrift.Mutation]]],x$2: org.apache.cassandra.thrift.ConsistencyLevel)
      client.batch_mutate(keyToFamilyMutations, consistency)
    } else {
      throw new IllegalArgumentException("cannot perform batch operation on 0 length operation sequence")
    }
  }


  /**
   * performs the list of operations in batch using the default consistency
   */
  def batch(ops: Seq[Operation]): Unit = batch(ops, defaultConsistency)


  /**
   * implicitly coverts a consistency value to an int
   */
  private implicit def toThriftConsistency(c: Consistency): ConsistencyLevel = c.thriftValue

  /**
   * all calls which access the session should be wrapped within this method,
   * it will catch any exceptions and make sure the session is then removed
   * from the pool.
   */
  private def detect[T](f: => T) = try {
    f
  } catch {
    case t: Throwable => lastError = Some(t); throw t
  }

  private def Buffer[T](v: java.util.List[T]) = {
    scala.collection.JavaConversions.asBuffer(v)
  }
}
