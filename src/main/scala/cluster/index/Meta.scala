package cluster.index

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import index._
import commands._
import scala.reflect.ClassTag

class Meta[T: ClassTag, K: ClassTag, V: ClassTag](val DATA_ORDER: Int,
                                                  val META_ORDER: Int)(implicit val ord: Ordering[K]) {

  val root = new AtomicReference[IndexRef[T, K, Partition[T, K, V]]](new IndexRef[T, K, Partition[T, K, V]](UUID
    .randomUUID.toString.asInstanceOf[T]))

  def find(k: K): Option[(K, Partition[T, K, V])] = {

    val old = root.get()
    val index = new Index[T, K, Partition[T, K, V]](old, DATA_ORDER, META_ORDER)

    QueryAPI.find(k, old.root)(index.ctx.parents) match {
      case None => None
      case Some(p) =>

        val block = p.asInstanceOf[DataBlock[T, K, V]]
        val (_, pos) = block.find(k, 0, block.size - 1)

        val (_, v) = block.keys(if(pos < block.size) pos else pos - 1)

        Some(k -> v.asInstanceOf[Partition[T, K, V]])
    }
  }

  def execute(cmd: Command[T, K, Partition[T, K, V]], index: Index[T, K, Partition[T, K, V]]): Boolean = {
    cmd match {
      case Add(data) => index.insert(data)._1
      case Put(data) => index.update(data)._1
      case Delete(keys) => index.remove(keys)._1
    }
  }

  def execute(commands: Seq[Command[T, K, Partition[T, K, V]]]): Boolean = {
    val old = root.get()
    val index = new Index[T, K, Partition[T, K, V]](old, DATA_ORDER, META_ORDER)

    val size = commands.length

    for(i<-0 until size){
      if(!execute(commands(i), index)) return false
    }

    root.compareAndSet(old, index.ref)
  }

  def isEmpty(): Boolean = {
    val old = root.get()
    val index = new Index[T, K, Partition[T, K, V]](old, DATA_ORDER, META_ORDER)

    index.isEmpty()
  }

  def isFull(): Boolean = {
    val old = root.get()
    val index = new Index[T, K, Partition[T, K, V]](old, DATA_ORDER, META_ORDER)

    index.isFull()
  }

  def inOrder(): Seq[(K, Partition[T, K, V])] = {
    QueryAPI.inOrder(root.get().root)
  }

}
