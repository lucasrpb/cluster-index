package cluster.index

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag
import commands._

class Client[T: ClassTag, K: ClassTag, V: ClassTag](val DATA_ORDER: Int,
                                                    val META_ORDER: Int,
                                                    val meta: Meta[T, K, V])(implicit val ord: Ordering[K]) {

  /*def parseInsert(data: Seq[(K, V)], insertions: TrieMap[Partition[T, K, V], Seq[(K, V)]]): Unit = {

    val sorted = data.sortBy(_._1)
    val size = sorted.length
    var pos = 0

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
  }

  def parseRemove(keys: Seq[K], removals: TrieMap[Partition[T, K, V], Seq[K]]): Unit = {

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
  }

  def parseUpdate(data: Seq[(K, V)], updates: TrieMap[Partition[T, K, V], Seq[(K, V)]]): Unit = {

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
  }

  def execute(insert: Seq[(K, V)], remove: Seq[K], update: Seq[(K, V)]): Boolean = {

    val insertions = TrieMap[Partition[T, K, V], Seq[(K, V)]]()
    val removals = TrieMap[Partition[T, K, V], Seq[K]]()
    val updates = TrieMap[Partition[T, K, V], Seq[(K, V)]]()

    parseInsert(insert, insertions)
    parseRemove(remove, removals)
    parseUpdate(update, updates)

    val commands = TrieMap[Partition[T, K, V], Seq[Command[T, K, V]]]()

    if(meta.isEmpty()){

      println(s"no partition...\n")

      val p = new Partition[T, K, V](DATA_ORDER, META_ORDER, meta)
      return p.execute(Seq(
        Insert(insert),
        Delete(remove),
        Update(update)
      ))
    }

    insertions.foreach { case (p, list) =>
      commands.put(p, Seq[Command[T, K, V]](Insert(list)))
    }

    removals.foreach { case (p, keys) =>
      commands.get(p) match {
        case None => commands.put(p, Seq[Command[T, K, V]](Delete(keys)))
        case Some(cmds) => commands.update(p, cmds :+ Delete[T, K, V](keys))
      }
    }

    updates.foreach { case (p, list) =>
      commands.get(p) match {
        case None => commands.put(p, Seq[Command[T, K, V]](Update(list)))
        case Some(cmds) => commands.update(p, cmds :+ Update[T, K, V](list))
      }
    }

    commands.foreach { case (p, cmds) =>
      p.execute(cmds)
    }

    true
  }*/

  def insert(data: Seq[(K, V)]): Boolean = {

    val insertions =  TrieMap[Partition[T, K, V], Seq[(K, V)]]()

    val sorted = data.sortBy(_._1)
    val size = sorted.length
    var pos = 0

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

    if(meta.isEmpty()){

      println(s"no partition...\n")

      val p = new Partition[T, K, V](DATA_ORDER, META_ORDER, meta)
      return p.execute(Seq(Insert(data)))
    }

    insertions.foreach { case (p, list) =>
      p.execute(Seq(Insert(list)))
    }

    true
  }

  def remove(keys: Seq[K]): Boolean = {

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

    if(removals.isEmpty){
      return false
    }

    removals.foreach { case (p, keys) =>
      p.execute(Seq(Delete(keys)))
    }

    true
  }

  def update(data: Seq[(K, V)]): Boolean = {

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

    if(updates.isEmpty){
      return false
    }

    updates.foreach { case (p, list) =>
      p.execute(Seq(Update(list)))
    }

    true
  }

  def inOrder(): Seq[(K, V)] = {
    meta.inOrder().foldLeft(Seq.empty[(K, V)]){ case (p, (_, n)) =>
      p ++ n.inOrder()
    }
  }

}
