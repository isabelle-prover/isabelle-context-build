/* Title: remote_build_job.scala
   Author: Fabian Huch, TU Muenchen

Command-line interface to execute build job on a remote Isabelle process.
 */

package isabelle


object Remote_Build_Job {
  object Encode {
    def option(opt: Option[JSON.T]): JSON.T = opt.orNull

    def time(time: Time): JSON.T = time.ms
    def timing(timing: Timing): JSON.T =
      JSON.Object(
        "elapsed" -> time(timing.elapsed),
        "cpu" -> time(timing.cpu),
        "gc" -> time(timing.gc))

    def process_result(res: Process_Result): JSON.T =
      JSON.Object(
        "rc" -> res.rc,
        "out_lines" -> res.out_lines,
        "err_lines" -> res.err_lines,
        "timing" -> timing(res.timing))
  }

  object Decode {
    def err[A](json: JSON.T, reason: String = ""): A =
      error("Invalid json: " + JSON.Format(json) +
        (if (reason.nonEmpty) ". Reason: " + reason else ""))

    def field(json: JSON.T, name: String): JSON.T =
      JSON.value(json, name) getOrElse err(json, "does not contain " + name)

    def double(json: JSON.T): Double =
      json match {
        case d: Double => d
        case _ => err(json, "not a number")
      }
    def string(json: JSON.T): String =
      json match {
        case s: String => s
        case _ => err(json, "not a string")
      }

    def option(opt: JSON.T): Option[JSON.T] = Option(opt)
    def list(json: JSON.T): List[JSON.T] =
      json match {
        case l: List[_] => l
        case _ => err(json, "not a list")
      }

    def time(json: JSON.T): Time = Time.ms(double(json).toLong)
    def timing(json: JSON.T): Timing =
      Timing(
        time(field(json, "elapsed")),
        time(field(json, "cpu")),
        time(field(json, "gc")))

    def process_result(json: JSON.T): Process_Result =
      Process_Result(
        double(field(json, "rc")).toInt,
        list(field(json, "out_lines")).map(string),
        list(field(json, "err_lines")).map(string),
        timing(field(json, "timing")))
  }

  def build_job(
    session_name: String,
    options: Options,
    dirs: List[Path],
    do_store: Boolean,
    numa_node: Option[Int],
  ): Process_Result = {
    val progress = new Progress

    val build_options =
      options +
        "completion_limit=0" +
        "editor_tracing_messages=0" +
        ("pide_reports=" + options.bool("build_pide_reports"))

    val store = Sessions.store(build_options)

    val selection = Sessions.Selection(sessions = List(session_name))

    val full_sessions = Sessions.load_structure(build_options, dirs = dirs)

    val deps =
      Sessions.deps(full_sessions.selection(selection), progress = progress,
        inlined_files = true).check_errors

    val log =
      build_options.string("system_log") match {
        case "" => No_Logger
        case "-" => Logger.make(progress)
        case log_file => Logger.make(Some(Path.explode(log_file)))
      }

    val session_setup: (String, Session) => Unit = (_, _) => ()

    val job =
      new Build_Job(progress, deps.background(session_name), store, do_store,
        new Resources(deps.background(session_name), log), session_setup, numa_node)

    job.join
  }

  val isabelle_tool: Isabelle_Tool = Isabelle_Tool("build_job",
    "run a single build job", Scala_Project.here,
    { args =>
      val build_options = Word.explode(Isabelle_System.getenv("ISABELLE_BUILD_OPTIONS"))

      var numa_node: Option[Int] = None
      var build_heap = false
      var dirs: List[Path] = Nil
      var options = Options.init(opts = build_options)

      val getopts = Getopts("""
Usage: isabelle build_job [OPTIONS] SESSION

  Options are:
    -N INT       numa node
    -b           build heap images
    -d DIR       include session directory
    -o OPTION    override Isabelle system OPTION (via NAME=VAL or NAME)

  Run a single build job.
""",
        "N:" -> (arg => numa_node = Some(Value.Int.parse(arg))),
        "b" -> (_ => build_heap = true),
        "d:" -> (arg => dirs = dirs ::: List(Path.explode(arg))),
        "o:" -> (arg => options = options + arg))

      val session_name =
        getopts(args) match {
          case session_name :: Nil => session_name
          case _ => getopts.usage()
        }

      val res =
        build_job(session_name = session_name, options = options, dirs = dirs,
          do_store = build_heap, numa_node = numa_node)

      val res_json = Encode.process_result(res)

      val progress = new Console_Progress()
      progress.echo(JSON.Format(res_json))
    })
}