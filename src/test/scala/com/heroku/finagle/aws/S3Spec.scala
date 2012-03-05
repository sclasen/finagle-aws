package com.heroku.finagle.aws

import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import Client._
import org.jboss.netty.buffer.ChannelBuffers
import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import com.heroku.finagle.aws.S3.{S3Secret, S3Key}
import com.twitter.logging.Logger
import com.twitter.logging.config.{LoggerConfig, ConsoleHandlerConfig}


class S3Spec extends WordSpec with MustMatchers {

  "An S3Client" should {

    "list buckets" in {
      val buckets: List[String] = ListAllBuckets(s3).get()
      buckets.length must be > 0
    }


    "delete items" in {
      DeleteBucket.deleteAllItemsInBucket(s3, bucket).get() must be(true)
    }

    "delete a bucket" in {
      s3(DeleteBucket(bucket)).get().getStatus must be(NO_CONTENT)
    }

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

    "head ok" in {
      val getResp = s3(Head(bucket, path)).get()
      getResp.getStatus must be(OK)
      getResp.getHeader(CONTENT_TYPE) must be("text/html")
    }

    "fail to get a non existent path " in {
      s3(Get(bucket + "failme", path)).get().getStatus must be(NOT_FOUND)
    }

    "list a bucket" in {
      val keys: List[String] = ListBucket.getKeys(s3, bucket)
      keys.length must be(1)
      keys.contains(path.substring(1)) must be(true)
    }

    "list all with and without a prefix" in {
      val list: List[String] = ListBucket.listAll(s3, bucket)(_.key).get
      list.length must be(1)
      list.contains(path.substring(1)) must be(true)
      val noList: List[String] = ListBucket.listAll(s3, bucket, Some(Prefix(prefixNotInPath)))(_.key).get
      noList.length must be(0)
      

    }

    /* "list big buckets" in {
      val bStart = System.currentTimeMillis()
      val bKeys = ListBucket.getKeys(s3, "heroku-jvm-proxy-central")
      println("Blocking:" + bKeys.length + " in " + (System.currentTimeMillis() - bStart))
      bKeys.length must be > 2000
      val nbStart = System.currentTimeMillis()
      val nbKeys = ListBucket.getKeysNonBlocking(s3, "heroku-jvm-proxy-central")
      nbKeys.get().length must be > 2000
      println("NBBlocking:" + nbKeys.get().length + " in " + (System.currentTimeMillis() - nbStart))
    }*/

    "delete ok" in {
      s3(Delete(bucket, path)).get().getStatus must be(NO_CONTENT)
    }

  }
}

object Client {

  import util.Properties._

  val logConf = new LoggerConfig {
    node = ""
    level = Logger.levelNames.get(configOr("LOG_LEVEL", "INFO"))
    handlers = List(new ConsoleHandlerConfig)
  }

  logConf.apply()

  val payload = "finagle testing 1 2 3"
  val path = "/some/test/key"
  val prefixNotInPath = "/not/some/test/key"
  val bucket = "test-finagle-bucket"
  lazy val s3 = S3.client(S3Key(config("S3_KEY")), S3Secret(config("S3_SECRET")))

  def config(key: String): String = {
    envOrNone(key).getOrElse(propOrNone(key).get)
  }

  def configOr(key: String, default: String): String = {
    envOrNone(key).getOrElse(propOrNone(key).getOrElse(default))
  }


}
