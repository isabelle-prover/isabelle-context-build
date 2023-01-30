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


  /* job configs */

  sealed abstract class Task_Def[A](name: String)

  case class Build_Task(
    session_name: String,
    session_background: Sessions.Background,
    store: Sessions.Store,
    do_store: Boolean,
    log: Logger,
    command_timings0: List[Properties.T]
  ) extends Task_Def[(Process_Result, Option[String])]("build|" + session_name)

  case class Present_Task(
    session_name: String,
    root_dir: Path,
    deps: Sessions.Deps,
    session: String,
    store: Sessions.Store,
    verbose: Boolean = false
  ) extends Task_Def[Process_Result]("present|" + session_name)


  /* execution context */

  abstract class Context(build: Build, progress: Progress) extends AutoCloseable {
    type Config

    abstract class Execution[A] protected[Context](val config: Config) {
      def terminate(): Unit
      def is_finished: Boolean
      def join: A
    }

    case class State(
      pending: Graph[String, Sessions.Info],
      presentation_sessions: List[String],
      running_builds: Map[String, (List[String], Execution[(Process_Result, Option[String])])]
        = Map.empty,
      running_presentations: Map[String, Execution[Process_Result]] = Map.empty
    ) {
      override def equals(that: Any): Boolean =
        that match {
          case s: State =>
            pending == s.pending && presentation_sessions == s.presentation_sessions &&
              running_builds.keys == s.running_builds.keys &&
              running_presentations.keys == s.running_presentations.keys
          case _ => false
        }

      def running: List[Execution[_]] =
        running_builds.values.map(_._2).toList ::: running_presentations.values.toList
    }

    def schedule_build(state: State): Option[(String, Config)]
    def schedule_presentation(state: State): Option[(String, Config)]
    def execute[A](session_name: String, config: Config, task: Task_Def[A]): Execution[A]
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

    val sorted = SortedSet(build.structure.build_graph.keys: _*)(Ordering)

    def schedule_build(state: State): Option[(String, Config)] =
      if (state.running_builds.size + state.running_presentations.size >= max_jobs) None
      else {
        def used_node(i: Int): Boolean =
          state.running_builds.iterator.exists(
            { case (_, (_, exec)) => exec.config.contains(i) })

        val next_build = state.pending.minimals.filterNot(state.running_builds.contains).toSet
        sorted.find(next_build.contains).map(session_name => session_name -> numa.next(used_node))
      }

    def schedule_presentation(state: State): Option[(String, Config)] =
      if (state.running_builds.size + state.running_presentations.size >= max_jobs) None
      else {
        val session = state.presentation_sessions.find(session =>
          !state.pending.keys.contains(session) && !state.running_presentations.contains(session))
        session.map(_ -> None)
      }

    class Build private[Local_Context](session_name: String, job: Build_Task, config: Config)
      extends Execution[(Process_Result, Option[String])](config) {
      private val build_job = new isabelle.Build_Job(
        progress, job.session_background, job.store, job.do_store, job.log, (_, _) => (),
        config, job.command_timings0)

      def join: (Process_Result, Option[String]) = build_job.join
      def terminate(): Unit = build_job.terminate()
      def is_finished: Boolean = build_job.is_finished
    }

    class Presentation private[Local_Context](present_job: Present_Task)
      extends Execution[Process_Result](None) {
      private var out = ""
      private var err = ""
      private val progress = new Progress {
        override def echo(msg: String): Unit = out += msg
        override def echo_warning(msg: String): Unit = err += msg
        override def echo_error_message(msg: String): Unit = err += msg
      }

      private val future_result = Future.thread("present", uninterruptible = true) {
        using(Export.open_database_context(present_job.store)) { database_context =>

          val context1 =
            Browser_Info.context(present_job.deps.sessions_structure,
              root_dir = present_job.root_dir, document_info =
                Document_Info.read(database_context, present_job.deps, List(present_job.session)))

          using(database_context.open_session(present_job.deps.background(present_job.session)))(
            Browser_Info.build_session(context1, _, progress = progress,
              verbose = present_job.verbose))
        }
      }

      def join: Process_Result = {
        val future_res = future_result.join_result
        val process_res = Process_Result(0, split_lines(out), split_lines(err))
        future_res match {
          case Exn.Res(()) => process_res
          case Exn.Exn(exn) => process_res.copy(rc = 1, err_lines =
            process_res.err_lines ::: split_lines(exn.toString))
        }
      }
      def terminate(): Unit = future_result.cancel()
      def is_finished: Boolean = future_result.is_finished
    }

    def execute[A](session_name: String, config: Config, job: Task_Def[A]): Execution[A] = job match {
      case build_job: Build_Task => new Build(session_name, build_job, config)
      case present_job: Present_Task => new Presentation(present_job)
    }

    def close(): Unit = ()
  }
}
