/* Title: slurm_context.scala
   Author: Fabian Huch and Mokhtar Riahi, TU Muenchen

Slurm scheduling and execution context for Isabelle build.
*/

package isabelle


import isabelle.Build_Scheduler.{Build, Build_Job, Context, Job, Present_Job, Scheduler}
import isabelle.Remote_Build_Job.{Decode, Encode, build_job}


object Slurm {
  class Slurm_Scheduler(
    worker_isabelle: Path,
    worker_home: Path,
    partitions: List[String],
    slurm_db: SQL.Database,
    strategy: Strategy
  ) extends Scheduler {
    def open_build_context(build: Build): Slurm_Context =
      new Slurm_Context(
        worker_isabelle, worker_home, partitions, slurm_db, strategy, build,
        build.progress)
  }

  class Slurm_Context(
    worker_isabelle: Path,
    worker_home: Path,
    partitions0: List[String],
    slurm_db: SQL.Database,
    strategy: Strategy,
    build: Build,
    progress: Progress
  ) extends Context(build, progress) {
    type Config = Slurm.Config

    val build_id = build.start.format(Date.Format("YYYY-MM-dd_HH-mm-ss_SSS"))

    def median[A](xs: List[A], default: A)(implicit ordering: Ordering[A]): A =
      if (xs.isEmpty) default
      else xs.sorted(ordering)(xs.length / 2)

    val compute_nodes = list_nodes

    /* distributed scheduling with partitions */

    val session_threads = Store.best_threads(slurm_db)
    val session_timings = Store.best_times(slurm_db)
    val median_time = median(session_timings.values.toList, Time.minutes(1))

    def time(session_name: String): Time =
      session_timings.getOrElse(session_name, median_time)

    val partition_sizes =
      compute_nodes.filter(_.partition.exists(partitions0.contains)).groupBy(_.partition.get).map {
        case (partition, configs) => partition -> configs.length * configs.head.threads
      }
    val partitions = partitions0.filter(partition_sizes.get(_).exists(_ > 0))

    val critical_time = Time.minutes(30)
    val critical_nodes = {
      val critical_maximals =
        build.structure.build_graph.node_depth(time(_).ms).filter(r =>
          r._2 > critical_time.ms && build.structure.build_graph.is_maximal(r._1))
      build.structure.build_graph.all_preds(critical_maximals.keys.toList).toSet
    }

    def threads_fast(session_name: String): Int =
      session_threads.getOrElse(session_name, 8)

    def threads_parallel(session_name: String): Int =
      session_timings.get(session_name) match {
        case Some(time) if time.seconds < 20.0 => 1
        case Some(time) if time.seconds < 60.0 => 2
        case Some(time) if time.seconds < 240.0 => 4
        case _ => 8
      }

    def distribute(
      session_name: String, num_threads: String => Int, cpus: List[(String, Int)]
    ): Option[(String, Config)] = {
      val threads = num_threads(session_name)
      cpus.find(_._2 >= threads).map(_._1).map(p => session_name ->
        Config(Some(p), threads, Memory.gib(8)))
    }

    def available_cpus(state: State): Map[String, Int] = {
      val running_configs =
        state.running_builds.values.map(_._2.config) ++
          state.running_presentations.values.map(_.config)
      partition_sizes.map { case (partition, cpus) =>
        partition ->
          (cpus - running_configs.filter(_.partition.contains(partition)).map(_.threads).sum)
      }
    }

    def prepare(state: State): Strategy_Args = {
      val cpus = available_cpus(state)

      val node_height =
        state.pending.new_node("", null).add_deps_acyclic("", state.pending.maximals).node_height(
          time(_).ms)
      val max_height = node_height.values.max

      val paths = state.pending.minimals.filterNot(state.running_builds.contains).map(node =>
        node -> node_height(node)).sortBy(_._2).reverse

      Strategy_Args(state.pending.keys, partitions.map(p => p -> cpus(p)), max_height, paths)
    }

    def schedule_build(state: State): Option[(String, Config)] =
      strategy.apply(this, prepare(state))

    def schedule_presentation(state: State): Option[(String, Config)] = {
      val session = state.presentation_sessions.find(session =>
        !state.pending.keys.contains(session) && !state.running_presentations.contains(session))
      session.flatMap { session_name =>
        val cpus = available_cpus(state)
        val pending_builds = state.pending.keys.map(threads_fast)
        val free =
          pending_builds.foldLeft(cpus) {
            case (cpus, threads) => partitions.find(cpus(_) >= threads) match {
              case Some(partition) => cpus.updated(partition, cpus(partition) - threads)
              case None => cpus.map(t => t._1 -> 0)
            }
          }
        partitions.find(free(_) > 0).map(partition =>
          session_name -> Config(Some(partition), 1, Memory.gib(4)))
      }
    }


    /* execution on slurm cluster */

    def list_nodes: List[Config] = {
      val res = Isabelle_System.bash(
        "sinfo " + Bash.string("--partition=" + partitions0.mkString(",")) + " --json")
      if (!res.ok) error("Could not get cluster state: " + res)

      val nodes =
        for {
          obj <- JSON.Object.unapply(JSON.parse(res.out))
          nodes <- JSON.list(
            obj, "nodes", json => for {
              obj <- JSON.Object.unapply(json)
              cpus <- JSON.int(obj, "cpus")
              memory <- JSON.int(obj, "real_memory")
              partitions <- JSON.strings(obj, "partitions")
              partition <- partitions.headOption
            } yield Config(Some(partition), cpus, Memory.kib(memory)))
        } yield nodes

      nodes match {
        case None => error("Bad JSON info:\n" + JSON.Format.pretty_print(JSON.parse(res.out)))
        case Some(Nil) => error("No resources in cluster")
        case Some(nodes) => nodes
      }
    }

    abstract class Slurm_Job[A] private[Slurm_Context](config: Config, job_id: String)
      extends Execution[A](config) {
      val id = build_id + "/" + job_id
      private var terminated = false

      lazy val isabelle_command: List[String]

      def get_result(res: Process_Result): A

      private val future_result: Future[Process_Result] = {
        val isabelle = worker_isabelle + Path.explode("bin/isabelle")
        val sopts =
          "--job-name=" + id ::
            "--nodes=1" ::
            "--ntasks=1" ::
            ("--cpus-per-task=" + config.threads) ::
            ("--mem=" + config.memory.kib.toInt + "K") ::
            ("--export=USER_HOME=" + File.symbolic_path(worker_home)) ::
            ("--chdir=" + File.symbolic_path(worker_isabelle)) ::
            config.partition.map(p => List("--partition=" + p)).getOrElse(Nil)

        val cmd = "srun" :: sopts ::: isabelle.implode :: isabelle_command

        Future.thread("distributed_build", uninterruptible = true) {
          val res = Isabelle_System.bash(Bash.strings(cmd))
          if (terminated) res.copy(rc = Process_Result.RC.interrupt) else res
        }
      }

      def terminate(): Unit = {
        terminated = true
        Isabelle_System.bash("scancel " + Bash.string("--name=" + id))
      }

      def is_finished: Boolean = future_result.is_finished

      def join: A = get_result(future_result.join)
    }

    class Slurm_Build private[Slurm_Context](
      session_name: String,
      job: Build_Job,
      config: Config
    ) extends Slurm_Job[(Process_Result, Option[String])](config, "build/" + session_name) {
      lazy val info = job.session_background.sessions_structure(session_name)
      lazy val dirs =
        info.dirs.filter(Sessions.is_session_dir).map(File.symbolic_path(_))

      lazy val isabelle_command: List[String] =
        "build_job" ::
          ("-o threads=" + config.threads) ::
          (if (job.do_store) List("-b") else Nil) :::
          dirs.map("-d " + Bash.string(_)) :::
          session_name ::
          Nil

      def get_result(res: Process_Result): (Process_Result, Option[String]) = {
        val json = res.out_lines.mkString("\n")
        Exn.capture(JSON.parse(json, strict = false)) match {
          case Exn.Exn(_) =>
            error("Could not read remote build job response: " + quote(json) +
              ". Error log: " + res.err)
          case Exn.Res(json) =>
            val (res1, digest) = Decode.tuple2(json)
            val res2 = Decode.process_result(res1)
            val digest1 = Decode.option(digest).map(Decode.string)
            (res2.copy(timing = res2.timing.copy(elapsed = res.timing.elapsed)), digest1)
        }
      }

      override def join: (Process_Result, Option[String]) = {
        val (result, heap_digest) = super.join
        Store.udpate(slurm_db, session_name, build_id, config, result.timing)
        (result, heap_digest)
      }
    }

    class Slurm_Presentation(
      session_name: String,
      job: Present_Job,
      config: Config,
    ) extends Slurm_Job[Process_Result](config, "present/" + session_name) {
      lazy val isabelle_command: List[String] =
        List("presentation", "-P " + Bash.string(File.symbolic_path(job.root_dir)), session_name)

      def get_result(res: Process_Result): Process_Result = res
    }

    def execute[A](session_name: String, config: Config, job: Job[A]): Execution[A] =
      job match {
        case build_job: Build_Job => new Slurm_Build(session_name, build_job, config)
        case present_job: Present_Job => new Slurm_Presentation(session_name, present_job, config)
      }

    def close(): Unit = {
      val res = Isabelle_System.bash("squeue --json " + Bash.string("--jobs=" + build_id)).check
      val jobs =
        for {
          obj <- JSON.Object.unapply(JSON.parse(res.out))
          jobs <- JSON.list(
            obj, "jobs", json => for {
              obj <- JSON.Object.unapply(json)
              id <- JSON.int(obj, "job_id")
            } yield id)
        } yield jobs
      jobs.foreach(_.foreach(id => Isabelle_System.bash("scancel " + Bash.string(id.toString)).check))
    }
  }

  case class Config(partition: Option[String], threads: Int, memory: Memory)

  implicit val time_ordering: Ordering[Time] = Ordering.by(_.ms)

  object Data {
    val database = Path.explode("$ISABELLE_HOME_USER/distributed_build.db")

    val session_name = SQL.Column.string("session_name").make_primary_key
    val build_id = SQL.Column.string("build_id").make_primary_key
    val partition = SQL.Column.string("partition")
    val time = SQL.Column.long("elapsed")
    val threads = SQL.Column.int("threads")

    val columns = List(session_name, build_id, partition, time, threads)
    val table = SQL.Table("distributed_build_log", columns)

    def where_equal(
      session_name: String,
      partition: Option[String] = None,
      threads: Option[Int] = None,
    ): SQL.Source =
      "WHERE " + (Data.session_name.equal(session_name) ::
        partition.map(_.toString).map(Data.partition.equal).toList :::
        threads.map(_.toString).map(Data.threads.equal).toList).mkString(" AND ")
  }

  object Store {
    val max_results = 30

    def open_db: SQL.Database = {
      val db = SQLite.open_database(Data.database)
      db.transaction(db.create_table(Data.table))
      db
    }

    def udpate(
      db: SQL.Database, session_name: String, build_id: String, config: Config, timing: Timing
    ): Unit =
      db.transaction {
        db.using_statement(Data.table.insert()) { stmt =>
          stmt.string(1) = session_name
          stmt.string(2) = build_id
          stmt.string(3) = config.partition.orNull
          stmt.long(4) = timing.elapsed.ms
          stmt.int(5) = config.threads
          stmt.execute()
        }

        val where_equal =
          Data.where_equal(
            session_name,
            partition = config.partition,
            threads = Some(config.threads))

        val build_ids =
          db.using_statement(Data.table.select(List(Data.build_id), where_equal))(stmt =>
            stmt.execute_query().iterator(_.string(Data.build_id)).toList)

        build_ids.sorted.dropRight(max_results).lastOption.foreach(build_id =>
          db.using_statement(Data.table.delete(sql =
            where_equal + " AND " + Data.build_id.ident + " < " + build_id))(
            _.execute()))
      }

    def best_threads(db: SQL.Database): Map[String, Int] =
      db.using_statement(Data.table
        .select(List(Data.session_name, Data.threads, Data.time)))(stmt =>
        stmt.execute_query().iterator(r =>
          r.string(Data.session_name) -> (r.long(Data.time) -> r.int(Data.threads))).toList
          .groupBy(
            _._1).map { case (s, ts) => s -> ts.map(_._2).minBy(_._1)._2 }
      )

    def best_times(db: SQL.Database): Map[String, Time] =
      db.using_statement(Data.table.select(List(Data.session_name, Data.time)))(stmt =>
        stmt.execute_query().iterator(r => r.string(Data.session_name) -> r.long(Data.time)).
          toList.groupBy(_._1).map { case (s, ts) => s -> Time.ms(ts.map(_._2).min) })
  }


  /* build strategies */

  case class Strategy_Args(
    running: List[String],
    partition_cpus: List[(String, Int)],
    max_height: Long,
    paths: List[(String, Long)]
  )

  sealed case class Strategy(
    name: String,
    description: String,
    apply: (Slurm_Context, Strategy_Args) => Option[(String, Config)]
  ) {
    override def toString: String = name
  }

  val known_strategies: List[Strategy] =
    List(
      Strategy(
        "path_prio", "builds sessions in longest paths of the graph first",
        { (sched, args) =>
          if (args.paths.isEmpty) None
          else if (2 * args.paths.last._2 <= args.max_height) {
            val (long, short :: _) = args.paths.partition(_._2 * 2 > args.max_height)

            long match {
              case Nil => sched.distribute(short._1, sched.threads_parallel, args.partition_cpus)
              case long :: _ => sched.distribute(long._1, sched.threads_fast, args.partition_cpus)
            }
          }
          else sched.distribute(args.paths.head._1, sched.threads_parallel, args.partition_cpus)
        }),
      Strategy(
        "partition_prio", "builds long paths on fast partitions and short paths on slow",
        { (sched, args) =>
          if (args.paths.isEmpty) None
          else if (2 * args.paths.last._2 <= args.max_height && args.partition_cpus.length > 1) {
            val (long, short :: _) = args.paths.partition(_._2 * 2 > args.max_height)
            val (fast, slow) = args.partition_cpus.splitAt((args.partition_cpus.length + 1) / 2)

            long.headOption.flatMap(s => sched.distribute(s._1, sched.threads_fast, fast)) orElse
              sched.distribute(short._1, sched.threads_parallel, slow)
          }
          else sched.distribute(args.paths.head._1, sched.threads_parallel, args.partition_cpus)
        }),
      Strategy(
        "absolute_prio", "builds paths >30m prioritized, except if <8 sessions are ready",
        { (sched, args) =>
          if (args.paths.isEmpty) None
          else if (args.paths.length + args.running.length < 8)
            sched.distribute(args.paths.head._1, sched.threads_fast, args.partition_cpus)
          else {
            val (critical, normal) = args.paths.map(_._1).partition(sched.critical_nodes.contains)

            critical match {
              case Nil =>
                normal.headOption.flatMap(
                  sched.distribute(_, sched.threads_parallel, args.partition_cpus))
              case critical :: _ =>
                sched.distribute(critical, sched.threads_fast, args.partition_cpus)
            }
          }
        })
    )

  def show_strategies: String =
    cat_lines(known_strategies.map(strategy => strategy.name + " - " + strategy.description))

  def the_strategy(name: String) =
    known_strategies.find(strategy => strategy.name == name) getOrElse
      error("Unknown strategy " + quote(name))
}