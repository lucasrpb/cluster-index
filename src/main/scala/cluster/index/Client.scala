package cluster.index

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag
import commands._

class Client[T: ClassTag, K: ClassTag, V: ClassTag](val DATA_ORDER: Int,
                                                    val META_ORDER: Int,
                                                    val meta: Meta[T, K, V])(implicit val ord: Ordering[K]) {

  def parseAdd(data: Seq[(K, V)], partitions: TrieMap[Partition[T, K, V], Seq[commands.Command[T, K, V]]]): Unit = {

    val sorted = data.sortBy(_._1)
    val size = sorted.length
    var pos = 0

    val insertions = TrieMap[Partition[T, K, V], Seq[(K, V)]]()

    while(pos < size){

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      val n = meta.find(k) match {
        case None => list.length
        case Some((max, p)) =>

          val idx = list.indexWhere {case (k, _) => ord.gt(k, max)}
          if(idx > 0) list = list.slice(0, idx)

          insertions.get(p) match {
            case None => insertions += p -> list
            case Some(l) => insertions.update(p, l ++ list)
          }

          list.length
      }

      pos += n
    }

    insertions.foreach { case (p, list) =>
      partitions.get(p) match {
        case None => partitions.put(p, Seq(Add(list)))
        case Some(cmds) => partitions.update(p, cmds :+ Add[T, K, V](list))
      }
    }
  }

  def parsePut(data: Seq[(K, V)], partitions: TrieMap[Partition[T, K, V], Seq[commands.Command[T, K, V]]]): Unit = {

    val updates = TrieMap[Partition[T, K, V], Seq[(K, V)]]()

    val sorted = data.sortBy(_._1)

    val size = sorted.length
    var pos = 0

    while(pos < size) {

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      val n = meta.find(k) match {
        case None => list.length
        case Some((max, p)) =>

          val idx = list.indexWhere {case (k, _) => ord.gt(k, max)}
          list = if(idx > 0) list.slice(0, idx) else list

          updates.get(p) match {
            case None => updates += p -> list
            case Some(l) => updates.update(p, l ++ list)
          }

          list.length
      }

      pos += n
    }

    updates.foreach { case (p, list) =>
      partitions.get(p) match {
        case None => partitions.put(p, Seq(Put(list)))
        case Some(cmds) => partitions.update(p, cmds :+ Put[T, K, V](list))
      }
    }
  }

  def parseDelete(keys: Seq[K], partitions: TrieMap[Partition[T, K, V], Seq[commands.Command[T, K, V]]]): Unit = {
    val removals = TrieMap[Partition[T, K, V], Seq[K]]()

    val sorted = keys.sorted

    val size = sorted.length
    var pos = 0

    while(pos < size) {

      var list = sorted.slice(pos, size)
      val k = list(0)

      val n = meta.find(k) match {
        case None => list.length
        case Some((max, p)) =>

          val idx = list.indexWhere {k => ord.gt(k, max)}
          list = if(idx > 0) list.slice(0, idx) else list

          removals.get(p) match {
            case None => removals += p -> list
            case Some(l) => removals.update(p, l ++ list)
          }

          list.length
      }

      pos += n
    }

    removals.foreach { case (p, keys) =>
      partitions.get(p) match {
        case None => partitions.put(p, Seq(Delete(keys)))
        case Some(cmds) => partitions.update(p, cmds :+ Delete[T, K, V](keys))
      }
    }
  }

  def execute(commands: Seq[Command[T, K, V]]): Boolean = {

    val partitions = TrieMap[Partition[T, K, V], Seq[Command[T, K, V]]]()

    if(meta.isEmpty()){
      println(s"\nno partition...\n")
      
      val p = new Partition[T, K, V](DATA_ORDER, META_ORDER, meta)
      return p.execute(commands)
    }

    commands.foreach { case cmd =>
      cmd match {
        case Add(data) => parseAdd(data, partitions)
        case Put(data) => parsePut(data, partitions)
        case Delete(keys) => parseDelete(keys, partitions)
      }
    }

    val metaBackup = meta.root.get()

    val backups = partitions.map { case (p, _) =>
      p -> p.root.get()
    }.toMap

    val codes = partitions.map { case (p, cmds) =>
      p.execute(cmds)
    }

    if(codes.exists(_ == false)){

      meta.root.set(metaBackup)

      partitions.foreach { case (p, _) =>
        p.root.set(backups(p))
      }

      return false
    }

    true
  }

  def inOrder(): Seq[(K, V)] = {
    meta.inOrder().foldLeft(Seq.empty[(K, V)]){ case (p, (_, n)) =>
      p ++ n.inOrder()
    }
  }

}
