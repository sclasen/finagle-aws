package com.heroku.finagle.aws

import org.specs2.mutable._
import com.twitter.finagle.builder.ClientBuilder
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import util.Properties
import Client._
import org.jboss.netty.buffer.ChannelBuffers

class S3Spec extends Specification {

  "An S3Client" should {
    "create a bucket " in {
      client(CreateBucket(bucket)).get().getStatus mustEqual OK
    }

    "put ok" in {
      client(Put(bucket, path, ChannelBuffers.wrappedBuffer(payload.getBytes), CONTENT_TYPE -> "text/html")).get().getStatus mustEqual OK
    }

    "fail to put into a bad bucket" in {
      client(Put(bucket + "failme", path, ChannelBuffers.wrappedBuffer(payload.getBytes), CONTENT_TYPE -> "text/html")).get().getStatus mustEqual NOT_FOUND
    }

    "get ok" in {
      val getResp = client(Get(bucket, path)).get()
      getResp.getStatus mustEqual OK
      getResp.getHeader(CONTENT_TYPE) mustEqual "text/html"
      new String(getResp.getContent.array()) mustEqual payload
    }

    "fail to get a non existent path " in {
      client(Get(bucket + "failme", path)).get().getStatus mustEqual NOT_FOUND
    }

    "delete ok" in {
      client(Delete(bucket, path)).get().getStatus mustEqual NO_CONTENT
    }

  }
}

object Client {
  val payload = "finagle testing 1 2 3"
  val path = "/some/test/key"
  val bucket = "test-finagle-bucket"
  lazy val client = ClientBuilder().hosts("s3.amazonaws.com:80")
                    .hostConnectionLimit(1).name("testClient")
                    .codec(S3.get(Properties.envOrNone("S3_KEY").get, Properties.envOrNone("S3_SECRET").get))
                    .build()
}
