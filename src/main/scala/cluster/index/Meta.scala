package cluster.index

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import commands._
import scala.reflect.ClassTag

class Meta[T: ClassTag, K: ClassTag, V: ClassTag](val MIN: Int,
                                                  val MAX: Int)(implicit val ord: Ordering[K]) {

  val root = new AtomicReference[Block[T, K, Partition[T, K, V]]](new Block[T, K, Partition[T, K, V]](UUID.randomUUID
    .toString.asInstanceOf[T], MIN, MAX))

  def find(k: K): Option[(K, Partition[T, K, V])] = {
    val index = root.get()

    if(index.isEmpty()) return None

    val p = index.asInstanceOf[Block[T, K, Partition[T, K, V]]]
    val (_, pos) = p.find(k, 0, p.size - 1)

    Some(p.keys(if(pos < p.size) pos else pos - 1))
  }

  def findSibling(k: K): Option[(K, Partition[T, K, V])] = {
    val index = root.get()

    if(index.isEmpty()) return None

    val (found, pos) = index.find(k, 0, index.size - 1)

    if(!found) return None

    var idx = pos + 1

    if(idx < index.size){
      return Some(index.keys(idx))
    }

    idx = pos - 1
    if(idx < 0) return None

    Some(index.keys(idx))
  }

  def execute(cmd: Command[T, K, Partition[T, K, V]], index: Block[T, K, Partition[T, K, V]]): Boolean = {
    cmd match {
      case Insert(data) => index.insert(data)._1
      case Update(data) => index.update(data)._1
      case Delete(keys) => index.remove(keys)._1
    }
  }

  def execute(commands: Seq[Command[T, K, Partition[T, K, V]]]): Boolean = {
    val old = root.get()
    val index = old.copy()

    val size = commands.length

    for(i<-0 until size){
      if(!execute(commands(i), index)) return false
    }

    root.compareAndSet(old, index)
  }

  def isEmpty(): Boolean = {
    root.get().isEmpty()
  }

  def isFull(): Boolean = {
    root.get().isFull()
  }

  def inOrder(): Seq[(K, Partition[T, K, V])] = {
    root.get().inOrder()
  }

}
