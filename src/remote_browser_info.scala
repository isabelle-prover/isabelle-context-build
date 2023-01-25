/* Title: remote_browser_info.scala
   Author: Fabian Huch, TU Muenchen

Command-line interface to build browser info on a remote Isabelle process.
 */

package isabelle


import Browser_Info.{Meta_Data, Node_Context}


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
        /*START*/
        progress.expose_interrupt()

        val session_name = session_context.session_name
        val session_info = session_context.sessions_structure(session_name)

        val session_dir = context.session_dir(session_name).expand
        progress.echo("Presenting " + session_name + " in " + session_dir + " ...")

        Meta_Data.init_directory(context.chapter_dir(session_name))
        Meta_Data.clean_directory(session_dir)

        val session = context.document_info.the_session(session_name)

        Bytes.write(
          session_dir + Browser_Info.session_graph_path,
          graphview.Graph_File.make_pdf(
            session_info.options,
            session_context.session_base.session_graph_display))

        val document_variants =
          for {
            doc <- session_info.document_variants
            db <- session_context.session_db()
            document <- Document_Build.read_document(db, session_name, doc.name)
          }
          yield {
            val doc_path = session_dir + doc.path.pdf
            if (Path.eq_case_insensitive(doc.path.pdf, Browser_Info.session_graph_path)) {
              error("Illegal document variant " + quote(doc.name) +
                " (conflict with " + Browser_Info.session_graph_path + ")")
            }
            if (verbose) progress.echo("Presenting document " + session_name + "/" + doc.name)
            if (session_info.document_echo) progress.echo("Document at " + doc_path)
            Bytes.write(doc_path, document.pdf)
            doc
          }

        val document_links = {
          val link1 = HTML.link(Browser_Info.session_graph_path, HTML.text("theory dependencies"))
          val links2 = document_variants.map(doc => HTML.link(doc.path.pdf, HTML.text(doc.name)))
          Library.separate(
            HTML.break ::: HTML.nl,
            (link1 :: links2).map(link => HTML.text("View ") ::: List(link))).flatten
        }

        def present_theory(theory_name: String): XML.Body = {
          progress.expose_interrupt()

          def err(): Nothing =
            error("Missing document information for theory: " + quote(theory_name))

          val snapshot = Build_Job.read_theory(session_context.theory(theory_name)) getOrElse err()
          val theory = context.theory_by_name(session_name, theory_name) getOrElse err()

          if (verbose) progress.echo("Presenting theory " + quote(theory_name))

          val thy_elements = theory.elements(context.elements)

          def node_context(file_name: String, node_dir: Path): Node_Context =
            Node_Context.make(context, session_name, theory_name, file_name, node_dir)

          val thy_html =
            context.source(
              node_context(theory.thy_file, session_dir).
                make_html(thy_elements, snapshot.xml_markup(elements = thy_elements.html)))

          val master_dir = Path.explode(snapshot.node_name.master_dir)

          val files =
            for {
              blob_name <- snapshot.node_files.tail
              xml = snapshot.switch(blob_name).xml_markup(elements = thy_elements.html)
              if xml.nonEmpty
            }
            yield {
              progress.expose_interrupt()

              val file = blob_name.node
              if (verbose) progress.echo("Presenting file " + quote(file))

              val file_html = session_dir + context.file_html(file)
              val file_dir = file_html.dir
              val html_link = HTML.relative_href(file_html, base = Some(session_dir))
              val html = context.source(node_context(file, file_dir).make_html(thy_elements, xml))

              val path = Path.explode(file)
              val src_path = File.relative_path(master_dir, path).getOrElse(path)

              val file_title = "File " + Symbol.cartouche_decoded(src_path.implode_short)
              HTML.write_document(
                file_dir, file_html.file_name,
                List(HTML.title(file_title)), List(context.head(file_title), html),
                root = Some(context.root_dir))
              List(HTML.link(html_link, HTML.text(file_title)))
            }

          val thy_title = "Theory " + theory.print_short
          HTML.write_document(
            session_dir, context.theory_html(theory).implode,
            List(HTML.title(thy_title)), List(context.head(thy_title), thy_html),
            root = Some(context.root_dir))

          List(HTML.link(
            context.theory_html(theory),
            HTML.text(theory.print_short) :::
              (if (files.isEmpty) Nil else List(HTML.itemize(files)))))
        }

        val theories = session.used_theories.map(present_theory)

        val title = "Session " + session_name
        HTML.write_document(
          session_dir, "index.html",
          List(HTML.title(title + Isabelle_System.isabelle_heading())),
          context.head(title, List(HTML.par(document_links))) ::
            context.contents("Theories", theories),
          root = Some(context.root_dir))

        Meta_Data.set_build_uuid(session_dir, session.build_uuid)
        /*END*/
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