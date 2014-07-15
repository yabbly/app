package com.studio6.app.model

import com.studio6.app.{model => m}

import org.joda.time.DateTime

import java.io.Serializable

/*
trait Id[T] {
  def value: T 
}

case class PostId(value: String) extends Id[String] 

trait Identifiable[T <: Id[_]] {
  def id: T 
} 

trait Auditable {
  def creationDate: DateTime
  def lastUpdatedDate: DateTime
}

trait Builder[T] {
  def build: T
}

trait Update[T <: Id[_]] {
  def id: T
}

trait Persisted[T <: Id[_]]
  extends Identifiable[T]
{
  def id: T
}

object User {
  case class Id(value: String) extends m.Id[String] 

  trait User {
    def name: Option[String]
    def email: Option[String]
  }

  case class Free(
      name: Option[String],
      email: Option[String])

  case class Persisted(
      id: Id,
      creationDate: DateTime,
      lastUpdatedDate: DateTime,
      name: Option[String],
      email: Option[String])
    extends m.Persisted[Id]
    with Auditable
    with User
  {
    def slug = name
  }

  case class Update(
      id: Id,
      name: Option[String],
      email: Option[String])
    extends m.Update[Id]

  class Builder(
      id: Id,
      var name: Option[String],
      var email: Option[String])
    extends m.Builder[Update]
  {
    def build: Update = new Update(id, name, email)
  }
}


case class Post(
    id: PostId,
    user: User,
    publishedDate: Option[DateTime],
    title: Option[String],
    body: String)
  extends Identifiable[PostId]
*/


/*
trait Entity {
  def creationDate: DateTime
  def lastUpdatedDate: DateTime
  def isActive: Boolean
}

trait Mutable {
  def creationDate: DateTime
  def lastUpdatedDateDate: DateTime
}
*/
