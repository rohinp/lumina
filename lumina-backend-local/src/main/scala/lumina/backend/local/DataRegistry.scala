package lumina.backend.local

import lumina.plan.backend.Row

/**
 * Holds named in-memory datasets that LocalBackend can read as sources.
 *
 * The `memory://` URI scheme used in ReadCsv paths is resolved against this
 * registry, enabling fully in-memory pipelines without touching the filesystem.
 * Actual CSV paths (anything not starting with "memory://") are resolved by the
 * CsvReader.
 */
final class DataRegistry private (sources: Map[String, Vector[Row]]):

  def register(path: String, rows: Vector[Row]): DataRegistry =
    new DataRegistry(sources + (path -> rows))

  /** Java/Kotlin-friendly overload — accepts any java.lang.Iterable of rows. */
  def register(path: String, rows: java.lang.Iterable[Row]): DataRegistry =
    import scala.jdk.CollectionConverters.*
    register(path, rows.asScala.toVector)

  def get(path: String): Option[Vector[Row]] = sources.get(path)

object DataRegistry:

  val empty: DataRegistry = new DataRegistry(Map.empty)

  /** Convenience: build a registry from a varargs list of (path, rows) pairs. */
  def of(entries: (String, Vector[Row])*): DataRegistry =
    entries.foldLeft(empty) { case (reg, (path, rows)) => reg.register(path, rows) }
