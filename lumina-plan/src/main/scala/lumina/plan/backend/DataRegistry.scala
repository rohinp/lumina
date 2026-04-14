package lumina.plan.backend

/**
 * Holds named in-memory datasets keyed by source path.
 *
 * Any backend that supports the `memory://` URI scheme resolves source paths
 * against this registry instead of the filesystem.  The registry is immutable;
 * each [[register]] call returns a new instance.
 *
 * {{{
 *   val registry = DataRegistry.of(
 *     "memory://customers" -> customerRows,
 *     "memory://orders"    -> orderRows
 *   )
 *   val backend = LocalBackend(registry)
 * }}}
 */
final class DataRegistry private (sources: Map[String, Vector[Row]]):

  def register(path: String, rows: Vector[Row]): DataRegistry =
    new DataRegistry(sources + (path -> rows))

  /** Java/Kotlin-friendly overload — accepts any java.lang.Iterable of rows. */
  def register(path: String, rows: java.lang.Iterable[Row]): DataRegistry =
    import scala.jdk.CollectionConverters.*
    register(path, rows.asScala.toVector)

  def get(path: String): Option[Vector[Row]] = sources.get(path)

  /** Returns all registered (path → rows) entries, used by backends to materialise tables. */
  def allEntries: Map[String, Vector[Row]] = sources

object DataRegistry:

  val empty: DataRegistry = new DataRegistry(Map.empty)

  /** Convenience: build a registry from a varargs list of (path, rows) pairs. */
  def of(entries: (String, Vector[Row])*): DataRegistry =
    entries.foldLeft(empty) { case (reg, (path, rows)) => reg.register(path, rows) }
