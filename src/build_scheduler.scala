/* Title:    build_scheduler.scala
   Author:   Fabian Huch, TU Muenchen

Abstract scheduler module for individual build and presentation jobs, with implementation for simple
local context.
 */

package isabelle


import scala.collection.immutable.SortedSet


object Build_Scheduler {
  /* scheduler */

  case class Build(
    start: Date,
    store: Sessions.Store,
    structure: Sessions.Structure,
    deps: Sessions.Deps,
    browser_info: Browser_Info.Config,
    progress: Progress)

  trait Scheduler { def open_build_context(build: Build): Context }

  class Local_Scheduler(max_jobs: Int, numa: NUMA.Nodes) extends Scheduler {
    def make_session_timing(
      sessions_structure: Sessions.Structure,
      timing: Map[String, Double]
    ): Map[String, Double] = {
      val maximals = sessions_structure.build_graph.maximals.toSet

      def desc_timing(session_name: String): Double = {
        if (maximals.contains(session_name)) timing(session_name)
        else {
          val descendants = sessions_structure.build_descendants(List(session_name)).toSet
          val g = sessions_structure.build_graph.restrict(descendants)
          (0.0 :: g.maximals.flatMap { desc =>
            val ps = g.all_preds(List(desc))
            if (ps.exists(p => !timing.isDefinedAt(p))) None
            else Some(ps.map(timing(_)).sum)
          }).max
        }
      }

      timing.keySet.iterator.map(name => (name -> desc_timing(name))).toMap.withDefaultValue(0.0)
    }

    def open_build_context(build: Build): Context = {
      val names = build.structure.build_graph.keys

      val timings = names.map(name =>
        (name, Context_Build.load_timings(build.progress, build.store, name)))
      val session_timing =
        make_session_timing(build.structure,
          timings.map({ case (name, (_, t)) => (name, t) }).toMap)
      new Local_Context(max_jobs, numa, session_timing, build, build.progress)
    }
  }


  /* task descriptions */

  sealed abstract class Task_Def(val name: String)

  case class Build_Task(
    session_name: String,
    do_store: Boolean = false,
    log: Logger = Logger.make(None),
    command_timings0: List[Properties.T] = Nil,
    input_heaps: List[String] = Nil,
  ) extends Task_Def("build|" + session_name)

  case class Present_Task(
    session_name: String,
    root_dir: Path = Path.current,
    verbose: Boolean = false
  ) extends Task_Def("presentation|" + session_name)


  /* execution context */

  abstract class Context(build: Build, progress: Progress) extends AutoCloseable {
    type Config

    abstract class Job protected[Context](val task: Task_Def, val config: Config) {
      def terminate(): Unit
      def is_finished: Boolean
      def join: Process_Result
    }

    class State private(
      val pending: Graph[String, Task_Def],
      val running: Map[String, Job]
    ) {
      def run(task: Task_Def, config: Config): State =
        new State(pending, running + (task.name -> execute(task, config)))
      def del(task: Task_Def): State =
        new State(pending.del_node(task.name), running - task.name)
    }

    object State {
      def init: State = {
        val graph = build.structure.build_graph
        val structure = build.deps.sessions_structure

        val presentation_sessions =
          for {
            name <- structure.build_topological_order
            info = graph.get_node(name)
            if build.browser_info.enabled(info)
          } yield name

        val entries: Iterator[((String, Task_Def), List[String])] =
          graph.keys_iterator.flatMap { session_name =>
            val build_task = Build_Task(session_name)
            val deps = graph.imm_preds(session_name).toList.map(Build_Task(_))
            val presentation_node =
              if (presentation_sessions.contains(session_name)) {
                val present_task = Present_Task(session_name)
                Some((present_task.name, present_task), List(build_task.name))
              } else None

            ((build_task.name, build_task), deps.map(_.name)) :: presentation_node.toList
          }
        new State(Graph.make(entries.toList, converse = true), Map.empty)
      }
    }

    def schedule(state: State): Option[(Task_Def, Config)]
    def execute(task: Task_Def, config: Config): Job
  }

  class Local_Context(
    max_jobs: Int,
    numa: NUMA.Nodes,
    session_timing: Map[String, Double],
    build: Build,
    progress: Progress
  ) extends Context(build, progress) {
    type Config = Option[Int]

    object Ordering extends scala.math.Ordering[String] {
      def compare(name1: String, name2: String): Int =
        session_timing(name2) compare session_timing(name1) match {
          case 0 =>
            build.structure.build_graph.get_node(name2).timeout compare build.structure.build_graph.get_node(name1).timeout match {
              case 0 => name1 compare name2
              case ord => ord
            }
          case ord => ord
        }
    }

    val sorted = SortedSet(build.structure.build_graph.keys: _*)(Ordering).toList

    def schedule(state: State): Option[(Task_Def, Option[Int])] =
      if (state.running.size >= max_jobs) None
      else {
        def used_node(i: Int): Boolean = state.running.iterator.exists(_._2.config.contains(i))

        val next = state.pending.minimals.filterNot(state.running.contains).toSet

        for {
          task <- sorted.map(Build_Task(_)).find(task => next.contains(task.name)) orElse
            sorted.map(Present_Task(_)).find(task => next.contains(task.name))
        } yield task -> numa.next(used_node)
      }

    class Build_Job private[Local_Context](task: Build_Task, config: Config)
      extends Job(task, config) {
      private val build_job = new isabelle.Build_Job(
        progress, build.deps.background(task.session_name), build.store, task.do_store, task.log,
        (_, _) => (), config, task.command_timings0)

      def join: Process_Result = build_job.join._1
      def terminate(): Unit = build_job.terminate()
      def is_finished: Boolean = build_job.is_finished
    }

    class Present_Job private[Local_Context](task: Present_Task, config: Config)
      extends Job(task, config) {

      val progress = new Buffered_Progress()

      private val future_result = Future.thread("present", uninterruptible = true) {
        using(Export.open_database_context(build.store)) { database_context =>

          val context1 =
            Browser_Info.context(build.structure,
              root_dir = task.root_dir, document_info =
                Document_Info.read(database_context, build.deps, List(task.session_name)))

          using(database_context.open_session(build.deps.background(task.session_name)))(
            Browser_Info.build_session(context1, _, progress = progress, verbose = task.verbose))
        }
      }

      def join: Process_Result = {
        val future_res = future_result.join_result
        val process_res = Process_Result(0, split_lines(progress.out), split_lines(progress.err))
        future_res match {
          case Exn.Res(()) => process_res
          case Exn.Exn(exn) => process_res.copy(rc = 1, err_lines =
            process_res.err_lines ::: split_lines(exn.toString))
        }
      }
      def terminate(): Unit = future_result.cancel()
      def is_finished: Boolean = future_result.is_finished
    }

    def execute(task: Task_Def, config: Config): Job = task match {
      case task: Build_Task => new Build_Job(task, config)
      case task: Present_Task => new Present_Job(task, config)
    }

    def close(): Unit = ()
  }
}
