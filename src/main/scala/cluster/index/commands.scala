package cluster.index

package object commands {

  trait Command[T, K, V]

  case class Add[T, K, V](data: Seq[(K, V)]) extends Command[T, K, V]
  case class Put[T, K, V](data: Seq[(K, V)]) extends Command[T, K, V]
  case class Delete[T, K, V](keys: Seq[K]) extends Command[T, K, V]

  case class Read[T, K, V](keys: Seq[K]) extends Command[T, K, V]

}
