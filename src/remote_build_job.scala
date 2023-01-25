/* Title: remote_build_job.scala
   Author: Fabian Huch, TU Muenchen

Command-line interface to execute build job on a remote Isabelle process.
 */

package isabelle


object Remote_Build_Job {
  object Encode {
    def tuple(_1: JSON.T, _2: JSON.T): JSON.T = JSON.Object("_1" -> _1, "_2" -> _2)
    def tuple(_1: JSON.T, _2: JSON.T, _3: JSON.T): JSON.T =
      JSON.Object("_1" -> _1, "_2" -> _2, "_3" -> _3)

    def option(opt: Option[JSON.T]): JSON.T = opt.orNull

    def prop(prop: Properties.T): JSON.T = prop.map(t => tuple(t._1, t._2))

    def path(path: Path): JSON.T = File.symbolic_path(path)

    def options_declare(opt: Options.Opt): JSON.T =
      JSON.Object(
        "public" -> opt.public,
        "pos" -> prop(opt.pos),
        "name" -> opt.name,
        "typ_name" -> (
          opt.typ match {
            case Options.Bool => "bool"
            case Options.Int => "int"
            case Options.Real => "real"
            case Options.String => "string"
            case Options.Unknown => error("Cannot encode unkown opt type")
          }),
        "value" -> opt.value,
      "standard" -> (
        if (opt.standard_value.contains(opt.value)) option(None)
        else option(Some(option(opt.standard_value)))),
      "description" -> opt.description)

    def options(options: Options): JSON.T =
      JSON.Object(
        "options" -> options.opt_iterator.toList.map(t => options_declare(t._2)),
        "section" -> options.section)

    def sha1_digest(digest: SHA1.Digest): JSON.T = digest.toString

    def sessions_info(info: Sessions.Info): JSON.T =
      JSON.Object(
        "name" -> info.name,
        "chapter" -> info.chapter,
        "dir_selected" -> info.dir_selected,
        "pos" -> prop(info.pos),
        "groups" -> info.groups,
        "dir" -> path(info.dir),
        "parent" -> option(info.parent),
        "description" -> info.description,
        "directories" -> info.directories.map(path),
        "options" -> options(info.options),
        "imports" -> info.imports,
        "theories" -> info.theories.map(t => tuple(
          options(t._1), t._2.map(t =>
            tuple(t._1, prop(t._2))))),
        "global_theories" -> info.global_theories,
        "document_theories" -> info.document_theories.map(t => tuple(t._1, prop(t._2))),
        "document_files" -> info.document_files.map(t => tuple(path(t._1), path(t._2))),
        "export_files" -> info.export_files.map(t => tuple(path(t._1), t._2, t._3)),
        "export_classpath" -> info.export_classpath,
        "meta_digest" -> sha1_digest(info.meta_digest))

    def props(props: List[Properties.T]): JSON.T = props.map(prop)

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

    def boolean(json: JSON.T): Boolean =
      json match {
        case b: Boolean => b
        case _ => err(json, "not a boolean")
      }

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

    def list(json: JSON.T): List[JSON.T] =
      json match {
        case l: List[_] => l
        case _ => err(json, "not a list")
      }

    def option(opt: JSON.T): Option[JSON.T] = Option(opt)

    def tuple2(json: JSON.T): (JSON.T, JSON.T) = (field(json, "_1"), field(json, "_2"))

    def tuple3(json: JSON.T): (JSON.T, JSON.T, JSON.T) =
      (field(json, "_1"), field(json, "_2"), field(json, "_3"))

    def path(json: JSON.T): Path = Path.explode(string(json))

    def prop(json: JSON.T): Properties.T =
      list(json).map { json =>
        val (_1, _2) = tuple2(json)
        (string(_1), string(_2))
      }

    def options_declare(
      json: JSON.T
    ): (Boolean, Position.T, String, String, String, Option[Option[String]], String) = (
      boolean(field(json, "public")),
      prop(field(json, "pos")),
      string(field(json, "name")),
      string(field(json, "typ_name")),
      string(field(json, "value")),
      option(field(json, "standard")).map(option(_).map(string)),
      string(field(json, "description")))

    def options(json: JSON.T): Options = {
      val empty = Options.empty.set_section(string(field(json, "section")))
      list(field(json, "options")).foldLeft(empty) {
        case (options, json) =>
          val (public, pos, name, typ_name, value, standard, description) = options_declare(json)
          options.declare(public, pos, name, typ_name, value, standard, description)
      }
    }

    def sha1_digest(json: JSON.T): SHA1.Digest = SHA1.fake_digest(string(json))

    def sessions_info(json: JSON.T): Sessions.Info =
      Sessions.Info(
        string(field(json, "name")),
        string(field(json, "chapter")),
        boolean(field(json, "dir_selected")),
        prop(field(json, "pos")),
        list(field(json, "groups")).map(string),
        path(field(json, "dir")),
        option(field(json, "parent")).map(string),
        string(field(json, "description")),
        list(field(json, "directories")).map(path),
        options(field(json, "options")),
        list(field(json, "imports")).map(string),
        list(field(json, "theories")).map { json =>
          val t = tuple2(json)
          (options(t._1), list(t._2).map { json =>
            val t = tuple2(json)
            (string(t._1), prop(t._2))
          })
        },
        list(field(json, "global_theories")).map(string),
        list(field(json, "document_theories")).map { json =>
          val t = tuple2(json)
          (string(t._1), prop(t._2))
        },
        list(field(json, "document_files")).map { json =>
          val t = tuple2(json)
          (path(t._1), path(t._2))
        },
        list(field(json, "export_files")).map { json =>
          val t = tuple3(json)
          (path(t._1), double(t._2).toInt, list(t._3).map(string))
        },
        list(field(json, "export_classpath")).map(string),
        sha1_digest(field(json, "meta_digest")))

    def props(json: JSON.T): List[Properties.T] = list(json).map(prop)

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
  ): (Process_Result, Option[String]) = {
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
        log, session_setup, numa_node, Nil)

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

      val (res, heap_digest) =
        build_job(session_name = session_name, options = options, dirs = dirs,
          do_store = build_heap, numa_node = numa_node)

      val res_json = Encode.tuple(Encode.process_result(res), Encode.option(heap_digest))

      val progress = new Console_Progress()
      progress.echo(JSON.Format(res_json))
    })
}