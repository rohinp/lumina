package lumina.plan

enum DataType:
  case Int32,
    Int64,
    Float64,
    BooleanType,
    StringType,
    Unknown

final case class Column(name: String, dataType: DataType, nullable: Boolean = true)

final case class Schema(columns: Vector[Column]):
  lazy val columnNames: Vector[String] = columns.map(_.name)
