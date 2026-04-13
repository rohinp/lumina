package lumina.config

import munit.FunSuite

/**
 * Tests for BackendRegistry — describes how backends are registered, retrieved,
 * and what happens when a lookup fails.
 */
class BackendRegistrySpec extends FunSuite:

  test("an empty registry contains no backends"):
    assert(BackendRegistry.empty.names.isEmpty)

  test("registering a backend makes it retrievable by name"):
    import lumina.backend.local.LocalBackend
    val registry = BackendRegistry.empty.register(LocalBackend())
    assert(registry.get("local").isDefined)

  test("retrieving a backend that was not registered returns None"):
    assert(BackendRegistry.empty.get("spark").isEmpty)

  test("getOrFail raises an error when the backend name is not registered"):
    intercept[IllegalArgumentException]:
      BackendRegistry.empty.getOrFail("unknown")

  test("the default registry contains all bundled backends"):
    val registry = BackendRegistry.default()
    assert(registry.get("local").isDefined,  "local backend must be in default registry")
    assert(registry.get("polars").isDefined, "polars backend must be in default registry")
    assert(registry.get("spark").isDefined,  "spark backend must be in default registry")

  test("registering a backend with the same name replaces the existing entry"):
    import lumina.backend.local.LocalBackend
    val first  = LocalBackend()
    val second = LocalBackend()
    val registry = BackendRegistry.empty.register(first).register(second)
    assertEquals(registry.names.count(_ == "local"), 1)
