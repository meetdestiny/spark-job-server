package ooyala.common.akka.web

import spray.json._
import spray.json.DefaultJsonProtocol._

/**
 * NOTE: Most of this code is needed because spray-json does not convert a generic Map
 *   with any kind of value.
 */
object JsonUtils {
  // Allows the conversion of flexible Maps that hold ints, strings, lists, maps
  // Note that this implicit conversion will only apply in this scope....
  // we have to be careful to make implicits that convert Any no wider in scope than needed
  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case l: Long => JsNumber(l)
      case d: Double => JsNumber(d)
      case f: Float => JsNumber(f.toDouble)
      case s: String => JsString(s)
      case x: Seq[_] => seqFormat[Any].write(x)
      // Get the type of map keys from the first key, translate the rest the same way
      case m: Map[_, _] => m.keys.head match {
        case sym: Symbol =>
          val map = m.asInstanceOf[Map[Symbol, _]]
          val pairs = map.map { case (sym, v) => (sym.name -> write(v)) }
          JsObject(pairs)
        case s: String => mapFormat[String, Any].write(m.asInstanceOf[Map[String, Any]])
        case a: Any =>
          val map = m.asInstanceOf[Map[Any, _]]
          val pairs = map.map { case (sym, v) => (sym.toString -> write(v)) }
          JsObject(pairs)
      }
      case a: Array[_] => seqFormat[Any].write(a.toSeq)
      case b: Boolean if b == true => JsTrue
      case b: Boolean if b == false => JsFalse
      case p: Product =>  seqFormat[Any].write(p.productIterator.toSeq)
      case x => JsString(x.toString)
    }
    def read(value: JsValue) = value match {
      case JsNumber(n) => n.intValue()
      case JsString(s) => s
      case a: JsArray => listFormat[Any].read(value)
      case o: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case x => deserializationError("Do not understand how to deserialize " + x)
    }
  }

  def mapToJson(map: Map[String, Any], compact: Boolean = true): String = {
    val jsonAst = map.toJson
    if (compact) jsonAst.compactPrint else jsonAst.prettyPrint
  }

  def listToJson(list: Seq[Any], compact: Boolean = true): String = {
    val jsonAst = list.toJson
    if (compact) jsonAst.compactPrint else jsonAst.prettyPrint
  }

  def mapFromJson(json: String) = json.asJson.convertTo[Map[String, Any]]

  def listFromJson(json: String) = json.asJson.convertTo[Seq[Any]]
}
