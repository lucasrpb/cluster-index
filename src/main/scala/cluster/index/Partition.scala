package cluster.index

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import commands._
import scala.reflect.ClassTag
import index._

class Partition[T: ClassTag, K: ClassTag, V: ClassTag](val DATA_ORDER: Int,
                                                       val META_ORDER: Int,
                                                       val meta: Meta[T, K, V])(implicit val ord: Ordering[K]) {

  val root = new AtomicReference[IndexRef[T, K, V]](new IndexRef[T, K, V](UUID.randomUUID.toString.asInstanceOf[T]))

  def execute(cmd: Command[T, K, V], index: Index[T, K, V]): Boolean = {
    cmd match {
      case Add(data) =>
        val ok = index.insert(data)._1

        if(!ok)
        println(s"add => ${ok}\n")

        ok
      case Put(data) =>
        val ok = index.update(data)._1

        if(!ok)
        println(s"put => ${ok}\n")

        ok
      case Delete(keys) =>

        val ok = index.remove(keys)._1

        if(!ok)
        println(s"delete => ${ok}\n")

        ok
    }
  }

  def execute(commands: Seq[Command[T, K, V]]): Boolean = {

    val old = root.get()
    val index = new Index[T, K, V](old, DATA_ORDER, META_ORDER)

    val max = index.max
    val size = commands.length

    for(i<-0 until size){
      if(!execute(commands(i), index)) {

        println("ooopsss!!")

        return false
      }
    }

    if(meta.isEmpty()){

      if(index.isEmpty()) return true

      return meta.execute(Seq(
        Add(Seq(index.max.get -> this))
      )) && root.compareAndSet(old, index.ref)
    }

    // Fix the method isFull() in Index...
    if(index.isFull() /*&& index.root.get.asInstanceOf[MetaBlock[T, K, V]].size > 1*/){

      println(s"FULL PARTITION... ${index.size} ${index.MAX}\n")

      val r = index.split().asInstanceOf[Index[T, K, V]]
      var cmds = Seq.empty[Command[T, K, Partition[T, K, V]]]

      val right = new Partition[T, K, V](DATA_ORDER, META_ORDER, meta)
      right.root.set(r.ref)

      if(max.isDefined){
        cmds = cmds :+ Delete[T, K, Partition[T, K, V]](Seq(max.get))
      }

      cmds = cmds :+ Add[T, K, Partition[T, K, V]](Seq(
        index.max.get -> this,
        r.max.get -> right
      ))

      return meta.execute(cmds) && root.compareAndSet(old, index.ref)
    }

    if(max.isDefined && index.isEmpty()){

      println(s"EMPTY PARTITION...\n")

      return meta.execute(Seq(Delete(Seq(max.get)))) && root.compareAndSet(old, index.ref)
    }

    val nmax = index.max

    if(max.isDefined && nmax.isDefined && !ord.equiv(max.get, nmax.get)){
      return meta.execute(Seq(
        Delete(Seq(max.get)),
        Add(Seq(nmax.get -> this))
      )) && root.compareAndSet(old, index.ref)
    }

    root.compareAndSet(old, index.ref)
  }

  def inOrder(): Seq[(K, V)] = {
    QueryAPI.inOrder(root.get().root)
  }

}
