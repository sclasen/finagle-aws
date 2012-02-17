package com.heroku.finagle.aws

import com.twitter.finagle.builder.ClientBuilder
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import Client._
import org.jboss.netty.buffer.ChannelBuffers
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

class S3Spec extends WordSpec with MustMatchers {

  "An S3Client" should {
    "create a bucket " in {
      s3(CreateBucket(bucket)).get().getStatus must be(OK)
    }

    "put ok" in {
      s3(Put(bucket, path, ChannelBuffers.wrappedBuffer(payload.getBytes), CONTENT_TYPE -> "text/html")).get().getStatus must be(OK)
    }

    "fail to put into a bad bucket" in {
      s3(Put(bucket + "failme", path, ChannelBuffers.wrappedBuffer(payload.getBytes))).get().getStatus must be(NOT_FOUND)
    }

    "get ok" in {
      val getResp = s3(Get(bucket, path)).get()
      getResp.getStatus must be(OK)
      getResp.getHeader(CONTENT_TYPE) must be("text/html")
      new String(getResp.getContent.array()) must be(payload)
    }

    "fail to get a non existent path " in {
      s3(Get(bucket + "failme", path)).get().getStatus must be(NOT_FOUND)
    }

    "list a bucket" in {
      val keys: List[String] = ListBucket.getKeys(s3, bucket)
      keys.length must be > 1
      keys.contains(path.substring(1)) must be(true)
    }

    "delete ok" in {
      s3(Delete(bucket, path)).get().getStatus must be(NO_CONTENT)
    }

  }
}

object Client {

  import util.Properties._

  val payload = "finagle testing 1 2 3"
  val path = "/some/test/key"
  val bucket = "test-finagle-bucket"
  lazy val s3 = ClientBuilder().hosts("s3.amazonaws.com:80")
    .hostConnectionLimit(1).name("testClient")
    .codec(S3.get(config("S3_KEY"), config("S3_SECRET")))
    .build()

  def config(key: String): String = {
    envOrNone(key).getOrElse(propOrNone(key).get)
  }

}
