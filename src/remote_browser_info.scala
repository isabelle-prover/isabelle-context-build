package isabelle


object Remote_Browser_Info {

  def build(
    session_name: String,
    browser_info: Browser_Info.Config,
    options: Options,
    dirs: List[Path] = Nil,
    progress: Progress = new Progress,
    verbose: Boolean = false
  ): Unit = {
    val store = Sessions.store(options)

    val sessions = List(session_name)
    val selection = Sessions.Selection(sessions = sessions)

    val full_sessions = Sessions.load_structure(options, dirs = dirs)

    val deps =
      Sessions.deps(full_sessions.selection(selection), progress = progress,
        inlined_files = true).check_errors

    val root_dir = browser_info.presentation_dir(store).absolute

    using(Export.open_database_context(store)) { database_context =>
      val context =
        Browser_Info.context(
          deps.sessions_structure, root_dir = root_dir,
          document_info = Document_Info.read(database_context, deps, sessions))

      using(database_context.open_session(deps.background(session_name))) { session_context =>
        Browser_Info.build_session(context, session_context, progress = progress, verbose = verbose)
      }
    }

    progress.echo("Presentation of " + session_name + " in " + root_dir)
  }

  val isabelle_tool: Isabelle_Tool = Isabelle_Tool("presentation", "run a single presentation",
    Scala_Project.here,
    { args =>
      val build_options = Word.explode(Isabelle_System.getenv("ISABELLE_BUILD_OPTIONS"))

      var browser_info = Browser_Info.Config.none
      var options = Options.init(opts = build_options)

      val getopts = Getopts(
        """
Usage: isabelle presentation [OPTIONS] SESSION

  Options are:
    -P DIR       build HTML/PDF presentation in directory (":" for default)
    -o OPTION    override Isabelle system OPTION (via NAME=VAL or NAME)

  Run a single presentation job.
""",
        "P:" -> (arg => browser_info = Browser_Info.Config.make(arg)),
        "o:" -> (arg => options = options + arg))

      val session_name =
        getopts(args) match {
          case session_name :: Nil => session_name
          case _ => getopts.usage()
        }

      val progress = new Console_Progress()
      val res = build(session_name = session_name, browser_info = browser_info, options = options,
        progress = progress)
    })
}