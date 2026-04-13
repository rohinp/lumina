package lumina.api

import lumina.plan.Schema
import lumina.plan.backend.{Backend, Row}
import java.util.{List as JList, Optional}

/**
 * Java-idiomatic entry point for the Lumina API.
 *
 * Mirrors [[Lumina]] but replaces Scala-specific types with their Java
 * equivalents so that Java and Kotlin callers do not need any Scala imports:
 *
 * {{{
 * // Java example
 * var backend = new LocalBackend(DataRegistry.empty().register("memory://t", rows));
 * var result  = LuminaJava.readCsv("memory://t")
 *                         .filter(new GreaterThan(new ColumnRef("age"), new Literal(30)))
 *                         .groupBy(List.of(new ColumnRef("city")),
 *                                  List.of(new Sum(new ColumnRef("revenue"), Optional.of("total"))))
 *                         .collectAsList(backend);
 * }}}
 */
object LuminaJava:

  /** Create a DataFrame that will read from the CSV file at the given path. */
  def readCsv(path: String): DataFrame =
    Lumina.readCsv(path)

  /** Create a DataFrame that will read from the CSV file at the given path with a known schema. */
  def readCsv(path: String, schema: Schema): DataFrame =
    Lumina.readCsv(path, schema)

  /** Create a DataFrame that will read from the CSV file at the given path with an optional schema. */
  def readCsv(path: String, schema: Optional[Schema]): DataFrame =
    Lumina.readCsv(path, schema)
