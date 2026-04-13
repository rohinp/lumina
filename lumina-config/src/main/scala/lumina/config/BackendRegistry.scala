package lumina.config

import lumina.plan.backend.Backend

/**
 * Registry of available backends, keyed by name.
 *
 * Backends are registered at startup and retrieved by name when a
 * LuminaSession is created.  The registry is immutable; each `register` call
 * returns a new registry.
 */
final class BackendRegistry private (backends: Map[String, Backend]):

  def register(backend: Backend): BackendRegistry =
    new BackendRegistry(backends + (backend.name -> backend))

  def get(name: String): Option[Backend] = backends.get(name)

  def getOrFail(name: String): Backend =
    get(name).getOrElse(
      throw IllegalArgumentException(
        s"No backend registered with name '$name'. Available: ${backends.keys.mkString(", ")}"
      )
    )

  def names: Set[String] = backends.keySet

object BackendRegistry:

  val empty: BackendRegistry = new BackendRegistry(Map.empty)

  /** Build a registry pre-loaded with all Lumina-bundled backends. */
  def default(): BackendRegistry =
    import lumina.backend.local.LocalBackend
    import lumina.backend.polars.PolarsBackend
    import lumina.backend.spark.SparkBackend
    empty
      .register(LocalBackend())
      .register(new PolarsBackend)
      .register(new SparkBackend)
