package cluster.index

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag
import commands._

class Client[T: ClassTag, K: ClassTag, V: ClassTag](val DATA_ORDER: Int,
                                                    val META_ORDER: Int,
                                                    val meta: Meta[T, K, V])(implicit val ord: Ordering[K]) {
  def insert(data: Seq[(K, V)]): Boolean = {

    val sorted = data.sortBy(_._1)
    val size = sorted.length
    var pos = 0

    val partitions = TrieMap[Partition[T, K, V], Seq[(K, V)]]()

    while(pos < size){

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      val n = meta.find(k) match {
        case None => list.length
        case Some((max, p)) =>

          val idx = list.indexWhere {case (k, _) => ord.gt(k, max)}
          if(idx > 0) list = list.slice(0, idx)

          partitions.get(p) match {
            case None => partitions += p -> list
            case Some(l) => partitions.update(p, l ++ list)
          }

          list.length
      }

      pos += n
    }

    if(partitions.isEmpty){

      println(s"no partition...\n")

      val p = new Partition[T, K, V](DATA_ORDER, META_ORDER, meta)
      return p.execute(Seq(Insert(data)))
    }

    partitions.foreach { case (p, data) =>
      p.execute(Seq(Insert(data)))
    }

    true
  }

  def inOrder(): Seq[(K, V)] = {
    meta.inOrder().foldLeft(Seq.empty[(K, V)]){ case (p, (_, n)) =>
      p ++ n.inOrder()
    }
  }

}
