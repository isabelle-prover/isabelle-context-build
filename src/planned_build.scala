/* Title: planned_build.scala
   Author: Fabian Huch, TU Muenchen

Engine for pre-planned builds, controlled by master.
 */
package isabelle


import isabelle.Build_Process.private_data.{Generic, build_uuid_tables, read_builds, read_serial, stamp_worker, start_build, start_worker, stop_build}


object Planned_Build {
  case class Config(name: String, build_uuid: String, node: Host.Node_Info, threads: Int)
  type Configs = Map[String, Config]
  type Result = (Config, Time)

  abstract class Strategy_Engine(name: String) extends Build.Engine(name) {
    protected var _build_configs: Configs = Map()


    /* build strategy */

    def best_result(name: String, previous: List[Result]): Option[Result] =
      previous.filter(_._1.name == name) match {
        case Nil => None
        case elems => Some(elems.minBy(_._2.ms))
      }

    def ready_jobs(state: Build_Process.State): List[String] =
      state.pending.filter(entry => entry.is_ready && !state.is_running(entry.name)).map(_.name)

    def running_configs(state: Build_Process.State): List[Config] =
      state.running.keys.map(_build_configs(_)).toList

    def host_configs(state: Build_Process.State, host: Build_Cluster.Host): List[Config] =
      running_configs(state).filter(_.name == host.name)

    def make_config(
      build_uuid: String,
      state: Build_Process.State,
      name: String,
      host: Build_Cluster.Host,
      threads: Int
    ): Config = {
      val numa_ident = Some(-(host_configs(state, host).length + 1))
      Config(name, build_uuid, node = Host.Node_Info(host.name, numa_ident), threads)
    }

    def total_path_time(graph: Graph[String, _], node_time: Map[String, Time]): Map[String, Time] = {
      val accumulated = graph.node_depth(node_time(_).ms).filter((name, _) => graph.is_maximal(name))
      accumulated.flatMap((name, ms) => graph.all_preds(List(name)).map(_ -> Time.ms(ms))).toMap
    }

    def parallel_paths(graph: Graph[String, _], node: String): Int =
      graph.imm_succs(node).map(succ => Math.max(1, parallel_paths(graph, succ))).sum

    def distribute(
      build_uuid: String,
      state: Build_Process.State,
      hosts: List[Build_Cluster.Host],
      jobs: List[(String, Int)]
    ): List[Config] =
      hosts match {
        case host :: hosts =>
          val capacity = host.jobs - host_configs(state, host).length

          val (_, configs, rest) =
            jobs.foldLeft((capacity, List.empty[Config], List.empty[(String, Int)])) {
              case ((capacity, configs, jobs), (name, threads)) =>
                if (capacity < threads) (capacity, configs, jobs :+ (name, threads))
                else {
                  val config = make_config(build_uuid, state, name, host, threads)
                  (capacity - threads, config :: configs, jobs)
                }
            }

          configs ::: distribute(build_uuid, state, hosts, rest)
        case Nil => Nil
      }

    def next(previous: List[Result], context: Build.Context, state: Build_Process.State): List[Config]


    /* build engine for strategy, controlled by build master */

    override def process_options(options: Options, node_info: Host.Node_Info): Options = {
      val options1 = super.process_options(options, node_info)
      val config = _build_configs.values.find(_.node == node_info).getOrElse(
        error("No config for process"))
      options1.int("threads") = config.threads
    }

    override def open_build_process(
      context: Build.Context,
      build_progress: Progress,
      server: SSH.Server
    ): Build_Process = new Build_Process(context, build_progress, server) {
      private val previous_results =
        _build_database.toList.flatMap { db =>
          val results = Build_Process.private_data.read_results(db)
          val configs = Planned_Build.private_data.read_configs(db).map(config =>
            (config.name, config.build_uuid) -> config).toMap

          results.values.map { result =>
            val config =
              configs.get((result.name, result.build_uuid)) match {
                case Some(config) => config
                case None =>
                  val threads = Isabelle_Thread.max_threads()
                  Config(result.name, result.build_uuid, result.node_info, threads)
              }
            (config, result.process_result.timing.elapsed)
          }
        }

      protected def synchronized_configs[A](label: String)(body: => A): A =
        synchronized {
          _build_database match {
            case None => body
            case Some(db) =>
              Planned_Build.private_data.transaction_lock(db, label = label) {
                _build_configs = Planned_Build.private_data.read_configs(db, context.build_uuid)
                  .map(config => config.name -> config).toMap
                val res = body
                Planned_Build.private_data.update_configs(db, context.build_uuid, _build_configs)
                res
              }
          }
        }

      override protected def next_jobs(state: Build_Process.State): List[String] =
        synchronized_configs("next_jobs") {
          if (context.master) {
            val next_builds = next(previous_results, context, state)
            _build_configs ++= next_builds.map(config => config.name -> config)
            next_builds.map(_.name)
          } else {
            state.pending
              .filter(entry => entry.is_ready && !state.is_running(entry.name))
              .map(_.name)
              .filter(_build_configs.get(_).exists(_.node.hostname == hostname))
          }
      }

      override protected def start_session(
        state: Build_Process.State,
        session_name: String
      ): Build_Process.State = {
        val build_uuid = context.build_uuid
        val ancestor_results =
          for (a <- state.sessions(session_name).ancestors) yield state.results(a)

        val sources_shasum = state.sessions(session_name).sources_shasum

        val input_shasum =
          if (ancestor_results.isEmpty) ML_Process.bootstrap_shasum()
          else SHA1.flat_shasum(ancestor_results.map(_.output_shasum))

        val store_heap =
          build_context.build_heap || Sessions.is_pure(session_name) ||
            state.sessions.iterator.exists(_.ancestors.contains(session_name))

        val (current, output_shasum) =
          store.check_output(
            _database_server, session_name,
            session_options = build_context.sessions_structure(session_name).options,
            sources_shasum = sources_shasum,
            input_shasum = input_shasum,
            fresh_build = build_context.fresh_build,
            store_heap = store_heap)

        val finished = current && ancestor_results.forall(_.current)
        val skipped = build_context.no_build
        val cancelled = progress.stopped || !ancestor_results.forall(_.ok)

        if (!skipped && !cancelled) {
          ML_Heap.restore(
            _database_server, session_name, store.output_heap(session_name),
            cache = store.cache.compress)
        }

        val result_name = (session_name, worker_uuid, build_uuid)

        if (finished) {
          state
            .remove_pending(session_name)
            .make_result(result_name, Process_Result.ok, output_shasum, current = true)
        }
        else if (skipped) {
          progress.echo("Skipping " + session_name + " ...", verbose = true)
          state.
            remove_pending(session_name).
            make_result(result_name, Process_Result.error, output_shasum)
        }
        else if (cancelled) {
          progress.echo(session_name + " CANCELLED")
          state
            .remove_pending(session_name)
            .make_result(result_name, Process_Result.undefined, output_shasum)
        }
        else {
          val config = _build_configs(session_name)
          val node_info = config.node

          progress.echo(
            (if (store_heap) "Building " else "Running ") + session_name +
              if_proper(node_info.numa_node, " on " + node_info) + " ...")

          val session = state.sessions(session_name)

          val build =
            Build_Job.start_session(
              build_context, session, progress, log, this.server,
              build_deps
                .background(session_name), sources_shasum, input_shasum, node_info, store_heap)

          val job = Build_Process.Job(session_name, worker_uuid, build_uuid, node_info, Some(build))

          state.add_running(job)
        }
      }
    }
  }

  object private_data extends SQL.Data("isabelle_planned_build"){
    object Configs {
      val name = Generic.name.make_primary_key
      val build_uuid = Generic.build_uuid.make_primary_key
      val hostname = SQL.Column.string("hostname")
      val numa_node = SQL.Column.int("numa_node")
      val threads = SQL.Column.int("threads")

      val table = make_table(List(name, build_uuid, hostname, numa_node, threads), name = "configs")
    }

    override def tables: SQL.Tables = SQL.Tables(Configs.table)

    def read_configs(db: SQL.Database, build_uuid: String = ""): List[Config] =
      db.execute_query_statement(Configs.table.select(sql =
        if_proper(build_uuid, Configs.build_uuid.where_equal(build_uuid))),
        List.from[Config],
        { res =>
          val name = res.string(Configs.name)
          val build_uuid = res.string(Configs.build_uuid)
          val hostname = res.string(Configs.hostname)
          val numa_node = res.get_int(Configs.numa_node)
          val threads = res.int(Configs.threads)
          Config(name, build_uuid, Host.Node_Info(hostname, numa_node), threads)
        })

    def update_configs(db: SQL.Database, build_uuid: String, configs: Configs): Boolean = {
      val old_configs = read_configs(db, build_uuid)
      val insert = configs.values.filterNot(old_configs.contains).toList

      for (config <- insert) {
        db.execute_statement(Configs.table.insert(), body =
          { stmt =>
            stmt.string(1) = config.name
            stmt.string(2) = build_uuid
            stmt.string(3) = config.node.hostname
            stmt.int(4) = config.node.numa_node
            stmt.int(5) = config.threads
          })
      }

      insert.nonEmpty
    }
  }


  /* heuristics */

  class Timing_Heuristic extends Strategy_Engine("timing_heuristic") {
    val slow = Time.minutes(30)

    def next(
      previous: List[Result],
      context: Build.Context,
      state: Build_Process.State
    ): List[Config] = {
      val build_uuid = context.build_uuid
      val ready = ready_jobs(state)
      val free = context.build_hosts.filter(host_configs(state, _).isEmpty)

      def best_threads(name: String): (Int, Time) =
        best_result(name, previous).map((config, time) => (config.threads, time)).getOrElse(
          (8, Time.minutes(5)))

      if (ready.length < free.length)
        ready.zip(free).map((name, host) =>
          make_config(build_uuid, state, name, host, best_threads(name)._1))
      else {
        val graph = state.sessions.graph
        val best_times = graph.keys.map(name => name -> best_threads(name)._2).toMap
        val path_time = total_path_time(graph, best_times)

        val (critical, other) = ready.sortBy(path_time(_).ms).partition(path_time(_).ms > slow.ms)
        val (critical_hosts, other_hosts) =
          context.build_hosts.splitAt(critical.map(parallel_paths(graph, _)).sum)

        distribute(build_uuid, state, critical_hosts, critical.map(name => name -> best_threads(name)._1)) :::
          distribute(build_uuid, state, other_hosts, other.map(_ -> 1))
      }
    }
  }
}