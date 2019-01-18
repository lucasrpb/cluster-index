package cluster.index

package object commands {

  trait Command[T, K, V]

  case class Insert[T, K, V](data: Seq[(K, V)]) extends Command[T, K, V]
  case class Update[T, K, V](data: Seq[(K, V)]) extends Command[T, K, V]
  case class Delete[T, K, V](keys: Seq[K]) extends Command[T, K, V]

}
