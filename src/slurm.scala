/* Title: slurm_context.scala
   Author: Fabian Huch and Mokhtar Riahi, TU Muenchen

Slurm scheduling and execution context for Isabelle build.
*/

package isabelle


import isabelle.Build_Scheduler.{Build, Build_Task, Context, Present_Task, Scheduler, Task_Def}
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

    val build_threads = Store.best_threads(slurm_db)
    val session_timings = Store.best_times(slurm_db)
    val median_time = median(session_timings.values.toList, Time.minutes(1))

    def time(name: String): Time = session_timings.getOrElse(name, median_time)

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

    def threads_default(name: String) = if (name.startsWith("build")) 8 else 1
    def threads_fast(name: String): Int = build_threads.getOrElse(name, threads_default(name))
    def threads_parallel(name: String): Int =
      session_timings.get(name) match {
        case Some(time) if time.seconds < 20.0 => 1
        case Some(time) if time.seconds < 240.0 => 4
        case _ => threads_default(name)
      }

    def available_cpus(state: State): Map[String, Int] = {
      partition_sizes.map {
        case (partition, cpus) =>
          val job_configs =
            state.running.values.map(_.config).filter(_.partition.contains(partition))
          partition -> (cpus - job_configs.map(_.threads).sum)
      }
    }

    def prepare(state: State): Strategy_Args = {
      val cpus = available_cpus(state)

      val node_height =
        state.pending.new_node("", null).add_deps_acyclic("", state.pending.maximals).node_height(
          time(_).ms)
      val max_height = node_height.values.max

      val paths = state.pending.minimals.filterNot(state.running.contains).map(
        node => node -> node_height(node)).sortBy(_._2).reverse

      def distribute(
        name: String, num_threads: String => Int, cpus: List[(String, Int)]
      ): Option[(Task_Def, Config)] = {
        val task = state.pending.get_node(name)
        val threads = num_threads(name)
        cpus.find(_._2 >= threads).map(_._1).map(p =>
          task -> Config(Some(p), threads, Memory.GiB(8)))
      }

      Strategy_Args(state.pending.keys, partitions.map(p => p -> cpus(p)), max_height, paths,
        distribute)
    }

    def schedule(state: State): Option[(Task_Def, Slurm.Config)] =
      strategy.apply(this, prepare(state))


    /* execution on slurm cluster */

    def list_nodes: List[Config] = {
      val res = Isabelle_System.bash(
        "sinfo --partition=" + Bash.string(partitions0.mkString(",")) + " --json")
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
            } yield Config(Some(partition), cpus, Memory.KiB(memory)))
        } yield nodes

      nodes match {
        case None => error("Bad JSON info:\n" + JSON.Format.pretty_print(JSON.parse(res.out)))
        case Some(Nil) => error("No resources in cluster")
        case Some(nodes) => nodes
      }
    }

    abstract class Slurm_Job private[Slurm_Context](
      task: Task_Def,
      config: Config,
    ) extends Job(task, config) {

      val id = build_id + "/" + task.name
      private var terminated = false

      lazy val isabelle_command: List[String]

      private val future_result: Future[Process_Result] = {
        val isabelle = worker_isabelle + Path.explode("bin/isabelle")
        val sopts =
          "--job-name=" + id ::
            "--nodes=1" ::
            "--ntasks=1" ::
            ("--cpus-per-task=" + config.threads) ::
            ("--mem=" + config.memory.KiB.toInt + "K") ::
            ("--export=USER_HOME=" + Bash.string(File.symbolic_path(worker_home))) ::
            ("--chdir=" + Bash.string(File.symbolic_path(worker_isabelle))) ::
            config.partition.map(p => List("--partition=" + Bash.string(p))).getOrElse(Nil)

        val cmd = "srun" :: sopts ::: isabelle.toString :: isabelle_command

        Future.thread("distributed_build", uninterruptible = true) {
          val res = Isabelle_System.bash(cmd.mkString(" "))
          if (terminated) res.copy(rc = Process_Result.RC.interrupt) else res
        }
      }

      def is_finished: Boolean = future_result.is_finished
      def join: Process_Result = future_result.join
      def terminate(): Unit = {
        terminated = true
        Isabelle_System.bash("scancel --name=" + Bash.string(id))
      }
    }

    class Slurm_Build private[Slurm_Context](
      task: Build_Task,
      config: Config
    ) extends Slurm_Job(task, config) {
      lazy val info = task.info
      lazy val dirs =
        info.dirs.filter(Sessions.is_session_dir).map(File.symbolic_path)

      lazy val isabelle_command: List[String] =
        "build_job" ::
          ("-o threads=" + config.threads) ::
          (if (task.do_store) List("-b") else Nil) :::
          dirs.map("-d " + Bash.string(_)) :::
          task.session_name ::
          Nil

      def get_result(res: Process_Result): Process_Result = {
        val json = res.out_lines.mkString("\n")
        Exn.capture(JSON.parse(json, strict = false)) match {
          case Exn.Exn(_) =>
            error("Could not read remote build job response: " + quote(json) +
              ". Error log: " + res.err)
          case Exn.Res(json) =>
            val res2 = Decode.process_result(json)
            res2.copy(timing = res2.timing.copy(elapsed = res.timing.elapsed))
        }
      }

      override def join: Process_Result = {
        val result = get_result(super.join)
        Store.udpate(slurm_db, task.name, build_id, config, result.timing)
        result
      }
    }

    class Slurm_Presentation(
      task: Present_Task,
      config: Config,
    ) extends Slurm_Job(task, config) {
      lazy val isabelle_command: List[String] =
        List("presentation", "-P " + Bash.string(File.symbolic_path(task.root_dir)), task.session_name)
    }

    def execute(task: Task_Def, config: Config): Job =
      task match {
        case task: Build_Task => new Slurm_Build(task, config)
        case task: Present_Task => new Slurm_Presentation(task, config)
      }

    def close(): Unit = {
      val res = Isabelle_System.bash("squeue --json --jobs=" + Bash.string(build_id)).check
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

    val name = SQL.Column.string("name").make_primary_key
    val build_id = SQL.Column.string("build_id").make_primary_key
    val partition = SQL.Column.string("partition")
    val time = SQL.Column.long("elapsed")
    val threads = SQL.Column.int("threads")

    val columns = List(name, build_id, partition, time, threads)
    val table = SQL.Table("distributed_build_log", columns)

    def where_equal(
      name: String,
      partition: Option[String] = None,
      threads: Option[Int] = None,
    ): SQL.Source =
      "WHERE " + (Data.name.equal(name) ::
        partition.map(Data.partition.equal).toList :::
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
      db: SQL.Database, name: String, build_id: String, config: Config, timing: Timing
    ): Unit =
      db.transaction {
        db.using_statement(Data.table.insert()) { stmt =>
          stmt.string(1) = name
          stmt.string(2) = build_id
          stmt.string(3) = config.partition.orNull
          stmt.long(4) = timing.elapsed.ms
          stmt.int(5) = config.threads
          stmt.execute()
        }

        val where_equal =
          Data.where_equal(name, partition = config.partition, threads = Some(config.threads))

        val build_ids =
          db.using_statement(Data.table.select(List(Data.build_id), where_equal))(stmt =>
            stmt.execute_query().iterator(_.string(Data.build_id)).toList)

        build_ids.sorted.dropRight(max_results).lastOption.foreach(build_id =>
          db.using_statement(Data.table.delete(sql =
            where_equal + " AND " + Data.build_id.ident + " < " + build_id))(
            _.execute()))
      }

    def best_threads(db: SQL.Database): Map[String, Int] =
      db.using_statement(Data.table.select(List(Data.name, Data.threads, Data.time)))(stmt =>
        stmt.execute_query().iterator(r =>
          r.string(Data.name) ->
            (r.long(Data.time) -> r.int(Data.threads))).toList.groupBy(_._1).map {
          case (s, ts) => s -> ts.map(_._2).minBy(_._1)._2 }
      )

    def best_times(db: SQL.Database): Map[String, Time] =
      db.using_statement(Data.table.select(List(Data.name, Data.time)))(stmt =>
        stmt.execute_query().iterator(r => r.string(Data.name) -> r.long(Data.time)).
          toList.groupBy(_._1).map { case (s, ts) => s -> Time.ms(ts.map(_._2).min) })
  }


  /* build strategies */

  case class Strategy_Args(
    running: List[String],
    partition_cpus: List[(String, Int)],
    max_height: Long,
    paths: List[(String, Long)],
    distribute: (String, String => Int, List[(String, Int)]) => Option[(Task_Def, Config)] 
  )

  sealed case class Strategy(
    name: String,
    description: String,
    apply: (Slurm_Context, Strategy_Args) => Option[(Task_Def, Config)]
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
              case Nil => args.distribute(short._1, sched.threads_parallel, args.partition_cpus)
              case long :: _ => args.distribute(long._1, sched.threads_fast, args.partition_cpus)
            }
          }
          else args.distribute(args.paths.head._1, sched.threads_parallel, args.partition_cpus)
        }),
      Strategy(
        "partition_prio", "builds long paths on fast partitions and short paths on slow",
        { (sched, args) =>
          if (args.paths.isEmpty) None
          else if (2 * args.paths.last._2 <= args.max_height && args.partition_cpus.length > 1) {
            val (long, short :: _) = args.paths.partition(_._2 * 2 > args.max_height)
            val (fast, slow) = args.partition_cpus.splitAt((args.partition_cpus.length + 1) / 2)

            long.headOption.flatMap(s => args.distribute(s._1, sched.threads_fast, fast)) orElse
              args.distribute(short._1, sched.threads_parallel, slow)
          }
          else args.distribute(args.paths.head._1, sched.threads_parallel, args.partition_cpus)
        }),
      Strategy(
        "absolute_prio", "builds paths >30m prioritized, except if <8 sessions are ready",
        { (sched, args) =>
          if (args.paths.isEmpty) None
          else if (args.paths.length + args.running.length < 8)
            args.distribute(args.paths.head._1, sched.threads_fast, args.partition_cpus)
          else {
            val (critical, normal) = args.paths.map(_._1).partition(sched.critical_nodes.contains)

            critical match {
              case Nil =>
                normal.headOption.flatMap(
                  args.distribute(_, sched.threads_parallel, args.partition_cpus))
              case critical :: _ =>
                args.distribute(critical, sched.threads_fast, args.partition_cpus)
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