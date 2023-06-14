package dqsuite.config

import com.typesafe.config._
import scala.collection.JavaConverters._
import scala.language.implicitConversions

/** A wrapper around a Typesafe Config object that provides a nicer API for loading values.
  */
private[dqsuite] trait ConfigLoader[A] {
  self =>
  def load(config: Config, path: String = ""): A

  def map[B](f: A => B): ConfigLoader[B] = (config, path) => f(self.load(config, path))
}

private[dqsuite] object ConfigLoader {
  def apply[A](f: Config => String => A): ConfigLoader[A] = f(_)(_)

  implicit val stringLoader: ConfigLoader[String]   = ConfigLoader(_.getString)
  implicit val intLoader: ConfigLoader[Int]         = ConfigLoader(_.getInt)
  implicit val booleanLoader: ConfigLoader[Boolean] = ConfigLoader(_.getBoolean)
  implicit val longLoader: ConfigLoader[Long]       = ConfigLoader(_.getLong)

  implicit val configLoader: ConfigLoader[Config]               = ConfigLoader(_.getConfig)
  implicit val configurationLoader: ConfigLoader[Configuration] = configLoader.map(Configuration)

  /** Loads a value, interpreting a null value as None and any other value as Some(value).
    */
  implicit def optionLoader[A](implicit valueLoader: ConfigLoader[A]): ConfigLoader[Option[A]] =
    (config, path) => if (config.getIsNull(path)) None else Some(valueLoader.load(config, path))

  implicit def seqLoader[A](implicit valueLoader: ConfigLoader[A]): ConfigLoader[Seq[A]] =
    (config, path) => {
      val list = config.getConfigList(path)

      list
        .iterator()
        .asScala
        .map { value => valueLoader.load(value, path) }
        .toSeq
    }

  implicit def mapLoader[A](implicit valueLoader: ConfigLoader[A]): ConfigLoader[Map[String, A]] =
    (config, path) => {
      val obj  = config.getObject(path)
      val conf = obj.toConfig

      obj
        .keySet()
        .iterator()
        .asScala
        .map { key =>
          key -> (conf.getValue(key).valueType() match {
            case ConfigValueType.OBJECT => valueLoader.load(conf.getConfig(key), path)
            case _                      => valueLoader.load(conf, key)
          })
        }
        .toMap
    }
}

private[dqsuite] case class Configuration(underlying: Config) {

  /** Get the config at the given path.
    */
  def get[A](path: String)(implicit loader: ConfigLoader[A]): A = {
    loader.load(underlying, path)
  }

  def getOptional[A](path: String)(implicit loader: ConfigLoader[A]): Option[A] = {
    if (underlying.hasPath(path)) {
      Some(loader.load(underlying, path))
    } else {
      None
    }
  }

  def getMap[A](path: String)(implicit loader: ConfigLoader[A]): Map[String, A] = {
    this.getOptional[Map[String, A]](path).getOrElse(Map.empty)
  }

  def getSeq[A](path: String)(implicit loader: ConfigLoader[A]): Seq[A] = {
    this.getOptional[Seq[A]](path).getOrElse(Seq.empty)
  }
}
