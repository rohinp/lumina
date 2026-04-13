package lumina.backend.local

import lumina.plan.backend.Row
import scala.io.Source
import scala.util.Using

/**
 * Minimal CSV reader used by LocalBackend for real file paths.
 *
 * - First row is treated as a header.
 * - Values are kept as strings; type coercion happens downstream via the schema.
 * - Blank lines are skipped.
 */
object CsvLoader:

  def load(path: String): Vector[Row] =
    Using(Source.fromFile(path)) { source =>
      val lines = source.getLines().filter(_.nonEmpty).toVector
      if lines.isEmpty then Vector.empty
      else
        val headers = lines.head.split(",").map(_.trim).toVector
        lines.tail.map { line =>
          val values = line.split(",", -1).map(_.trim)
          val pairs  = headers.zip(values).map { case (h, v) => h -> coerce(v) }
          Row(pairs.toMap)
        }
    }.get

  /** Best-effort coercion: try Int → Long → Double → Boolean → String. */
  private def coerce(raw: String): Any =
    raw.toIntOption
      .orElse(raw.toLongOption)
      .orElse(raw.toDoubleOption)
      .orElse(if raw.equalsIgnoreCase("true") || raw.equalsIgnoreCase("false") then Some(raw.toBoolean) else None)
      .getOrElse(raw)
