/* Title:     context_build.scala
   Author:    Fabian Huch, TU Muenchen

Isabelle build, adapted with execution context (e.g., for distributed execution on slurm).
 */

package isabelle


import Remote_Build_Job.{Decode, Encode}
import Build_Scheduler.{Build_Task, Local_Scheduler, Present_Task, Scheduler}
import Slurm.Slurm_Scheduler

import scala.collection.immutable.SortedSet
import scala.annotation.tailrec


object Context_Build {
  /* timing information */

  type Timings = (List[Properties.T], Double)

  def load_timings(progress: Progress, store: Sessions.Store, session_name: String): Timings = {
    val no_timings: Timings = (Nil, 0.0)

    store.try_open_database(session_name) match {
      case None => no_timings
      case Some(db) =>
        def ignore_error(msg: String) = {
          progress.echo_warning("Ignoring bad database " + db +
            " for session " + quote(session_name) + (if (msg == "") "" else ":\n" + msg))
          no_timings
        }
        try {
          val command_timings = store.read_command_timings(db, session_name)
          val session_timing =
            store.read_session_timing(db, session_name) match {
              case Markup.Elapsed(t) => t
              case _ => 0.0
            }
          (command_timings, session_timing)
        }
        catch {
          case ERROR(msg) => ignore_error(msg)
          case exn: java.lang.Error => ignore_error(Exn.message(exn))
          case _: XML.Error => ignore_error("XML.Error")
        }
        finally { db.close() }
    }
  }


  /** build with results **/

  class Results private[Context_Build](
    val store: Sessions.Store,
    val deps: Sessions.Deps,
    results: Map[String, (Option[Process_Result], Sessions.Info)]
  ) {
    def cache: Term.Cache = store.cache

    def sessions: Set[String] = results.keySet
    def cancelled(name: String): Boolean = results(name)._1.isEmpty
    def info(name: String): Sessions.Info = results(name)._2
    def apply(name: String): Process_Result = results(name)._1.getOrElse(Process_Result(1))
    val rc: Int =
      results.iterator.map({ case (_, (Some(r), _)) => r.rc case (_, (None, _)) => 1 }).
        foldLeft(Process_Result.RC.ok)(_ max _)
    def ok: Boolean = rc == Process_Result.RC.ok

    def unfinished: List[String] = sessions.iterator.filterNot(apply(_).ok).toList.sorted

    override def toString: String = rc.toString
  }

  def session_finished(session_name: String, process_result: Process_Result): String =
    "Finished " + session_name + " (" + process_result.timing.message_resources + ")"

  def session_timing(session_name: String, build_log: Build_Log.Session_Info): String = {
    val props = build_log.session_timing
    val threads = Markup.Session_Timing.Threads.unapply(props) getOrElse 1
    val timing = Markup.Timing_Properties.get(props)
    "Timing " + session_name + " (" + threads + " threads, " + timing.message_factor + ")"
  }

  def build(
    start_date: Date,
    scheduler: Scheduler,
    options: Options,
    selection: Sessions.Selection = Sessions.Selection.empty,
    browser_info: Browser_Info.Config = Browser_Info.Config.none,
    progress: Progress = new Progress,
    check_unknown_files: Boolean = false,
    build_heap: Boolean = false,
    clean_build: Boolean = false,
    dirs: List[Path] = Nil,
    select_dirs: List[Path] = Nil,
    infos: List[Sessions.Info] = Nil,
    list_files: Boolean = false,
    check_keywords: Set[String] = Set.empty,
    fresh_build: Boolean = false,
    no_build: Boolean = false,
    soft_build: Boolean = false,
    verbose: Boolean = false,
    verbose_presentation: Boolean = false,
    export_files: Boolean = false,
  ): Results = {
    val build_options =
      options +
        "completion_limit=0" +
        "editor_tracing_messages=0" +
        ("pide_reports=" + options.bool("build_pide_reports"))

    val store = Sessions.store(build_options)

    Isabelle_Fonts.init()


    /* session selection and dependencies */

    val full_sessions =
      Sessions.load_structure(build_options, dirs = dirs, select_dirs = select_dirs, infos = infos)
    val full_sessions_selection = full_sessions.imports_selection(selection)

    val build_deps = {
      val deps0 =
        Sessions.deps(full_sessions.selection(selection),
          progress = progress, inlined_files = true, verbose = verbose,
          list_files = list_files, check_keywords = check_keywords).check_errors

      if (soft_build && !fresh_build) {
        val outdated =
          deps0.sessions_structure.build_topological_order.flatMap(name =>
            store.try_open_database(name) match {
              case Some(db) =>
                using(db)(store.read_build(_, name)) match {
                  case Some(build)
                  if build.ok && build.sources == deps0.sources_shasum(name) => None
                  case _ => Some(name)
                }
              case None => Some(name)
            })

        Sessions.deps(full_sessions.selection(Sessions.Selection(sessions = outdated)),
          progress = progress, inlined_files = true).check_errors
      }
      else deps0
    }

    val build_graph = build_deps.sessions_structure.build_graph

    val names = build_graph.keys
    val timings = names.map(name => (name, load_timings(progress, store, name)))
    val command_timings0 =
      timings.map({ case (name, (ts, _)) => (name, ts) }).toMap.withDefaultValue(Nil)


    /* check unknown files */

    if (check_unknown_files) {
      val source_files =
        (for {
          (_, base) <- build_deps.session_bases.iterator
          (path, _) <- base.session_sources.iterator
        } yield path).toList
      Mercurial.check_files(source_files)._2 match {
        case Nil =>
        case unknown_files =>
          progress.echo_warning("Unknown files (not part of the underlying Mercurial repository):" +
            unknown_files.map(path => path.expand.implode).sorted.mkString("\n  ", "\n  ", ""))
      }
    }


    /* prepare presentation */

    val presentation_dir = browser_info.presentation_dir(store).absolute
    val presentation_context0 =
      Browser_Info.context(build_deps.sessions_structure, root_dir = presentation_dir)
    if (browser_info.enabled) { presentation_context0.update_root() }


    /* main build process */

    val build =
      Build_Scheduler.Build(start_date, store, build_deps.sessions_structure, build_deps,
        browser_info, progress)
    using(scheduler.open_build_context(build)) { ctx =>
      val state = ctx.State.init

      store.prepare_output_dir()

      if (clean_build) {
        for (name <- full_sessions.imports_descendants(full_sessions_selection)) {
          val (relevant, ok) = store.clean_output(name)
          if (relevant) {
            if (ok) progress.echo("Cleaned " + name)
            else progress.echo(name + " FAILED to clean")
          }
        }
      }

      // scheduler loop
      case class Result(
        current: Boolean,
        output_heap: SHA1.Shasum,
        process: Option[Process_Result],
        info: Sessions.Info
      ) {
        def ok: Boolean =
          process match {
            case None => false
            case Some(res) => res.ok
          }
      }

      def sleep(): Unit =
        Isabelle_Thread.interrupt_handler(_ => progress.stop()) {
          build_options.seconds("editor_input_delay").sleep()
        }

      val log =
        build_options.string("system_log") match {
          case "" => No_Logger
          case "-" => Logger.make(progress)
          case log_file => Logger.make(Some(Path.explode(log_file)))
        }

      @tailrec def loop(state: ctx.State, results: Map[String, Result]): Map[String, Result] =
        if (state.pending.is_empty) results
        else {
          if (progress.stopped) state.running.values.foreach(_.terminate())

          state.running.values.find(_.is_finished) match {
            case Some(job) =>
              //{{{ finish job
              val results1 = job.task match {
                case build: Build_Task =>
                  val session_name = build.session_name
                  val info = build_graph.get_node(build.session_name)
                  val process_result = job.join

                  val log_lines = process_result.out_lines.filterNot(Protocol_Message.Marker.test)
                  val process_result_tail = {
                    val tail = info.options.int("process_output_tail")
                    process_result.copy(
                      out_lines =
                        "(see also " + store.output_log(session_name).file.toString + ")" ::
                        (if (tail == 0) log_lines else log_lines.drop(log_lines.length - tail max 0)))
                  }

                  val build_log =
                    Build_Log.Log_File(session_name, process_result.out_lines).
                      parse_session_info(
                        command_timings = true,
                        theory_timings = true,
                        ml_statistics = true,
                        task_statistics = true)

                  // write log file
                  if (process_result.ok) {
                    File.write_gzip(store.output_log_gz(session_name), terminate_lines(log_lines))
                  }
                  else File.write(store.output_log(session_name), terminate_lines(log_lines))

                  val output_heap =
                    if (process_result.ok && build.do_store && store.output_heap(session_name).is_file) {
                      SHA1.shasum(ML_Heap.write_digest(store.output_heap(session_name)), session_name)
                    }
                    else SHA1.no_shasum

                  // write database
                  val sources = Sessions.Sources.load(build_deps.background(session_name).base,
                    cache = store.cache.compress)
                  using(store.open_database(session_name, output = true))(db =>
                    store.write_session_info(db, session_name, sources,
                      build_log =
                        if (process_result.timeout) build_log.error("Timeout") else build_log,
                      build =
                        Build.Session_Info(build_deps.sources_shasum(session_name),
                          build.input_heaps, output_heap, process_result.rc,
                          UUID.random().toString)))

                  // messages
                  process_result.err_lines.foreach(progress.echo)

                  if (process_result.ok) {
                    if (verbose) progress.echo(session_timing(session_name, build_log))
                    progress.echo(session_finished(session_name, process_result))
                  }
                  else {
                    progress.echo(session_name + " FAILED")
                    if (!process_result.interrupted) progress.echo(process_result_tail.out)
                  }
                  results + (session_name ->
                    Result(false, output_heap, Some(process_result_tail), info))

                case presentation: Present_Task =>
                  val session_name = presentation.session_name
                  val process_result = job.join
                  process_result.err_lines.foreach(progress.echo)

                  if (process_result.ok) {
                    val session_description =
                      build_deps.sessions_structure(session_name).description
                    presentation_context0.update_chapter(session_name, session_description)
                    progress.echo(
                      "Finished presenting " + session_name + " in " + presentation_dir)
                    results
                  }
                  else {
                    progress.echo("Presenting " + session_name + " FAILED")
                    results +
                      (session_name -> results(session_name).copy(process = Some(process_result)))
                  }
              }

              loop(state.del(job.task), results1)
              //}}}
            case None =>
              //{{{ check/start next build job
              ctx.schedule(state) match {
                case Some(task0: Build_Task, config) =>
                  val session_name = task0.session_name
                  val info = build_graph.get_node(session_name)
                  val ancestor_results =
                    build_deps.sessions_structure.build_requirements(List(session_name)).
                      filterNot(_ == session_name).map(results(_))
                  val input_heaps =
                    if (ancestor_results.isEmpty) {
                      SHA1.shasum_meta_info(SHA1.digest(Path.explode("$POLYML_EXE")))
                    }
                    else SHA1.flat_shasum(ancestor_results.map(_.output_heap))

                  val is_maximal_build = {
                    val succ_tasks =
                      state.pending.imm_succs(task0.name).toList.map(state.pending.get_node)
                    succ_tasks.collectFirst({ case build: Build_Task => build }).isEmpty
                  }

                  val do_store =
                    build_heap || Sessions.is_pure(session_name) || !is_maximal_build

                  val (current, output_heap) = {
                    store.try_open_database(session_name) match {
                      case Some(db) =>
                        using(db)(store.read_build(_, session_name)) match {
                          case Some(build) =>
                            val output_heap = store.find_heap_shasum(session_name)
                            val current =
                              !fresh_build &&
                              build.ok &&
                              build.sources == build_deps.sources_shasum(session_name) &&
                              build.input_heaps == input_heaps &&
                              build.output_heap == output_heap &&
                              !(do_store && output_heap.is_empty)
                            (current, output_heap)
                          case None => (false, SHA1.no_shasum)
                        }
                      case None => (false, SHA1.no_shasum)
                    }
                  }
                  val all_current = current && ancestor_results.forall(_.current)

                  if (all_current) {
                    loop(state.del(task0), results +
                      (session_name -> Result(true, output_heap, Some(Process_Result.ok), info)))
                  }
                  else if (no_build) {
                    progress.echo_if(verbose, "Skipping " + session_name + " ...")
                    loop(state.del(task0), results +
                      (session_name -> Result(false, output_heap, Some(Process_Result.error), info)))
                  }
                  else if (ancestor_results.forall(_.ok) && !progress.stopped) {
                    progress.echo(
                      (if (do_store) "Building " else "Running ") + session_name + " ...")

                    store.clean_output(session_name)
                    using(store.open_database(session_name, output = true))(
                      store.init_session_info(_, session_name))

                    val task = task0.copy(do_store = do_store, log = log, command_timings0 =
                      command_timings0(session_name), input_heaps = input_heaps)

                    loop(state.run(task, config), results)
                  }
                  else {
                    progress.echo(session_name + " CANCELLED")
                    loop(state.del(task0),
                      results + (session_name -> Result(false, output_heap, None, info)))
                  }
                case Some(task0: Present_Task, config) =>
                  val session_name = task0.session_name
                  if (results(session_name).current) {
                    progress.echo("Skipping presentation of " + session_name)
                    loop(state.del(task0), results)
                  } else if (results(session_name).ok && !progress.stopped) {
                    progress.echo("Presenting " + session_name)

                    val task =
                      task0.copy(root_dir = presentation_dir, verbose = verbose_presentation)
                    loop(state.run(task, config), results)
                  }
                  else {
                    progress.echo(session_name + " Presentation CANCELLED")
                    loop(state.del(task0), results)
                  }
                case None => sleep(); loop(state, results)
              }
              ///}}}
          }
        }


      /* build results */

      val results = {
        val build_results =
          if (build_deps.is_empty) {
            progress.echo_warning("Nothing to build")
            Map.empty[String, Result]
          }
          else Isabelle_Thread.uninterruptible { loop(state, Map.empty) }

        val results =
          (for ((name, result) <- build_results.iterator)
            yield (name, (result.process, result.info))).toMap

        new Results(store, build_deps, results)
      }

      if (export_files) {
        for (name <- full_sessions_selection.iterator if results(name).ok) {
          val info = results.info(name)
          if (info.export_files.nonEmpty) {
            progress.echo("Exporting " + info.name + " ...")
            for ((dir, prune, pats) <- info.export_files) {
              Export.export_files(store, name, info.dir + dir,
                progress = if (verbose) progress else new Progress,
                export_prune = prune,
                export_patterns = pats)
            }
          }
        }
      }

      if (!results.ok && (verbose || !no_build)) {
        progress.echo("Unfinished session(s): " + commas(results.unfinished))
      }

      results
    }
  }


  /* Isabelle tool wrapper */

  val isabelle_tool = Isabelle_Tool("context_build",
    "build and manage Isabelle sessions in an execution context",
    Scala_Project.here,
    { args =>
      val build_options = Word.explode(Isabelle_System.getenv("ISABELLE_BUILD_OPTIONS"))

      var base_sessions: List[String] = Nil
      var select_dirs: List[Path] = Nil
      var scheduler: Scheduler = new Local_Scheduler(1, new NUMA.Nodes(false))
      var browser_info = Browser_Info.Config.none
      var requirements = false
      var soft_build = false
      var verbose_presentation = false
      var exclude_session_groups: List[String] = Nil
      var all_sessions = false
      var build_heap = false
      var clean_build = false
      var dirs: List[Path] = Nil
      var export_files = false
      var fresh_build = false
      var session_groups: List[String] = Nil
      var cluster_dirs: List[Path] = Nil
      var check_keywords: Set[String] = Set.empty
      var list_files = false
      var no_build = false
      var options = Options.init(opts = build_options)
      var partitions: List[String] = Nil
      var strategy: Option[Slurm.Strategy] = None
      var verbose = false
      var exclude_sessions: List[String] = Nil

      val getopts = Getopts("""
Usage: isabelle contex_build [OPTIONS] [SESSIONS ...]

  Options are:
    -B NAME      include session NAME and all descendants
    -D DIR       include session directory and select its sessions
    -P DIR       enable HTML/PDF presentation in directory (":" for default)
    -R           refer to requirements of selected sessions
    -S           soft build: only observe changes of sources, not heap images
    -V           verbose presentation
    -X NAME      exclude sessions from group NAME and all descendants
    -a           select all sessions
    -b           build heap images
    -c           clean build
    -d DIR       include session directory
    -e           export files from session specification into file-system
    -f           fresh build
    -g NAME      select session group NAME
    -h DIRS      cluster directories (user_home:isabelle_home)
    -k KEYWORD   check theory sources for conflicts with proposed keywords
    -l           list session source files
    -n           no build -- test dependencies only
    -o OPTION    override Isabelle system OPTION (via NAME=VAL or NAME)
    -p NAMES     cluster partitions
    -s NAME      use named distribution strategy
    -v           verbose
    -x NAME      exclude session NAME and all descendants

  Build and manage Isabelle sessions locally or in a distributed context
  with one of the following distribution strategies:

""" + Library.indent_lines(4, Slurm.show_strategies) + """

  The build also depends on implicit settings:

""" + Library.indent_lines(2,  Build_Log.Settings.show()) + "\n",
        "B:" -> (arg => base_sessions = base_sessions ::: List(arg)),
        "D:" -> (arg => select_dirs = select_dirs ::: List(Path.explode(arg))),
        "P:" -> (arg => browser_info = Browser_Info.Config.make(arg)),
        "R" -> (_ => requirements = true),
        "S" -> (_ => soft_build = true),
        "V" -> (_ => verbose_presentation = true),
        "X:" -> (arg => exclude_session_groups = exclude_session_groups ::: List(arg)),
        "a" -> (_ => all_sessions = true),
        "b" -> (_ => build_heap = true),
        "c" -> (_ => clean_build = true),
        "d:" -> (arg => dirs = dirs ::: List(Path.explode(arg))),
        "e" -> (_ => export_files = true),
        "f" -> (_ => fresh_build = true),
        "g:" -> (arg => session_groups = session_groups ::: List(arg)),
        "h:" -> (arg => cluster_dirs = space_explode(':', arg).map(Path.explode)),
        "k:" -> (arg => check_keywords = check_keywords + arg),
        "l" -> (_ => list_files = true),
        "n" -> (_ => no_build = true),
        "o:" -> (arg => options = options + arg),
        "p:" -> (arg => partitions = Library.distinct(space_explode(',', arg))),
        "s:" -> (arg => strategy = Some(Slurm.the_strategy(arg))),
        "v" -> (_ => verbose = true),
        "x:" -> (arg => exclude_sessions = exclude_sessions ::: List(arg)))

      val sessions = getopts(args)

      val progress = new Console_Progress(verbose = verbose)

      val start_date = Date.now()

      if (verbose)
        progress.echo(
          "Started at " + Build_Log.print_date(start_date) +
            " (" + Isabelle_System.getenv("ML_IDENTIFIER") + " on " + Isabelle_System.hostname() +")")

      using(Slurm.Store.open_db) { db =>
        strategy.foreach { strategy =>
          val (user_home, isabelle_home) = cluster_dirs match {
            case user_home :: isabelle_home :: Nil => (user_home, isabelle_home)
            case _ => getopts.usage()
          }
          scheduler =
            new Slurm_Scheduler(isabelle_home, user_home, partitions, db, strategy)
        }

        val results =
          progress.interrupt_handler {
            build(
              start_date,
              scheduler,
              options,
              selection = Sessions.Selection(
                requirements = requirements,
                all_sessions = all_sessions,
                base_sessions = base_sessions,
                exclude_session_groups = exclude_session_groups,
                exclude_sessions = exclude_sessions,
                session_groups = session_groups,
                sessions = sessions),
              browser_info = browser_info,
              progress = progress,
              check_unknown_files = Mercurial.is_repository(Path.ISABELLE_HOME),
              build_heap = build_heap,
              clean_build = clean_build,
              dirs = dirs,
              select_dirs = select_dirs,
              list_files = list_files,
              check_keywords = check_keywords,
              fresh_build = fresh_build,
              no_build = no_build,
              soft_build = soft_build,
              verbose = verbose,
              verbose_presentation = verbose_presentation,
              export_files = export_files)
          }
        val end_date = Date.now()
        val elapsed_time = end_date.time - start_date.time

        if (verbose) {
          progress.echo("\nFinished at " + Build_Log.print_date(end_date))
        }

        val total_timing =
          results.sessions.iterator.map(a => results(a).timing).foldLeft(Timing.zero)(_ + _).
            copy(elapsed = elapsed_time)
        progress.echo(total_timing.message_resources)

        sys.exit(results.rc)
      }
    })
}

class Context_Build_Tool
  extends Isabelle_Scala_Tools(
    Context_Build.isabelle_tool,
    Remote_Build_Job.isabelle_tool,
    Remote_Browser_Info.isabelle_tool)
