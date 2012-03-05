package com.heroku.finagle.aws

import com.twitter.finagle.http.Http
import com.twitter.finagle.http.netty.{HttpResponseProxy, HttpRequestProxy}
import org.jboss.netty.channel._
import org.joda.time.format.DateTimeFormat
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import org.jboss.netty.handler.codec.http.HttpVersion._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import com.twitter.finagle._
import builder.ClientBuilder
import org.jboss.netty.handler.codec.http.HttpMethod._
import org.jboss.netty.handler.codec.http._
import org.joda.time.{DateTime, DateTimeZone}
import javax.crypto.spec.SecretKeySpec
import javax.crypto.Mac
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.logging.Logger
import org.jboss.netty.util.CharsetUtil._
import collection.mutable.HashMap
import annotation.{implicitNotFound, tailrec}
import com.twitter.util.{StorageUnit, Future}
import com.twitter.conversions.storage._
import xml.XML

object S3 {
  type S3Client = Service[S3Request, HttpResponse]

  @implicitNotFound(msg = "cannot find implicit S3Key in scope")
  case class S3Key(key: String)

  @implicitNotFound(msg = "cannot find implicit S3Secret in scope")
  case class S3Secret(secret: String)


  def get(key: String, secret: String) = new S3(key, secret)

  def client(key: S3Key, secret: S3Secret, name: String = "S3Client", maxReq: StorageUnit = 100.megabytes, maxRes: StorageUnit = 100.megabytes): S3Client = {
    ClientBuilder().codec(S3(key.key, secret.secret, Http(_maxRequestSize = maxReq, _maxResponseSize = maxRes)))
      .sendBufferSize(262144)
      .recvBufferSize(262144)
      .hosts("s3.amazonaws.com:80")
      .hostConnectionLimit(Integer.MAX_VALUE)
      .name(name)
      .build()
  }
}

case class S3(private val key: String, private val secret: String, httpFactory: CodecFactory[HttpRequest, HttpResponse] = Http.get()) extends CodecFactory[S3Request, HttpResponse] {

  def client = Function.const {
    new Codec[S3Request, HttpResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = httpFactory.client(null).pipelineFactory.getPipeline
          pipeline.addLast("requestEncoder", new RequestEncoder(key, secret))
          pipeline
        }
      }
    }
  }

  def server = throw new UnsupportedOperationException("This is a client side only codec factory")
}


class RequestEncoder(key: String, secret: String) extends SimpleChannelDownstreamHandler {

  val log = Logger.get(classOf[RequestEncoder])

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case bucketRequest: BucketRequest =>
        prepare(bucketRequest)
        ctx.sendDownstream(e)
      case serviceRequest: ServiceRequest =>
        prepare(serviceRequest)
        ctx.sendDownstream(e)
      case unknown =>
        ctx.sendDownstream(e)
    }
  }

  def prepare(req: BucketRequest) {
    req.setHeaders(HOST -> bucketHost(req.bucket), DATE -> amzDate)
    req.setHeaders(AUTHORIZATION -> authorization(key, secret, req))
    //Add query params after signing
    if (req.queries.size > 0)
      req.setUri(req.getUri + "?" + req.queries.map(qp => (qp._1 + "=" + qp._2)).reduceLeft(_ + "&" + _))
  }

  def prepare(req: ServiceRequest) {
    req.setHeaders(HOST -> "s3.amazonaws.com", DATE -> amzDate)
    req.setHeaders(AUTHORIZATION -> authorization(key, secret, req))
  }

  /*DateTime format required by AWS*/
  lazy val amzFormat = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss z").withLocale(java.util.Locale.US).withZone(DateTimeZone.forOffsetHours(0))

  /*headers used by this app that need to be used in signing*/
  //TODO make this extensible
  val SOURCE_ETAG = "x-amz-meta-source-etag"
  val SOURCE_MOD = "x-amz-meta-source-mod"
  val COPY_SOURCE = "x-amz-copy-source"
  val ACL = "x-amz-acl"
  val STORAGE_CLASS = "x-amz-storage-class"
  val VERSION = "x-amz-version-id"
  //headers need to be in alphabetical order in this list
  val AMZN_HEADERS = List(ACL, COPY_SOURCE, SOURCE_ETAG, SOURCE_MOD, STORAGE_CLASS, VERSION)

  val RRS = "REDUCED_REDUNDANCY"
  val ALGORITHM = "HmacSHA1"


  def amzDate: String = amzFormat.print(new DateTime)


  /*request signing for amazon*/
  /*Create the Authorization payload and sign it with the AWS secret*/
  def sign(secret: String, request: BucketRequest): String = {
    val data = List(
      request.getMethod().getName,
      request.header(CONTENT_MD5).getOrElse(""),
      request.header(CONTENT_TYPE).getOrElse(""),
      request.getHeader(DATE)
    ).foldLeft("")(_ + _ + "\n") + normalizeAmzHeaders(request) + "/" + request.bucket + request.getUri
    log.debug("String to sign")
    log.debug(data)
    calculateHMAC(secret, data)
  }


  def sign(secret: String, request: ServiceRequest): String = {
    val data = List(
      request.getMethod().getName,
      request.header(CONTENT_MD5).getOrElse(""),
      request.header(CONTENT_TYPE).getOrElse(""),
      request.getHeader(DATE)
    ).foldLeft("")(_ + _ + "\n") + normalizeAmzHeaders(request) + request.getUri
    log.debug("String to sign")
    log.debug(data)
    calculateHMAC(secret, data)
  }


  def normalizeAmzHeaders(request: S3Request): String = {
    AMZN_HEADERS.foldLeft("") {
      (str, h) => {
        request.header(h).flatMap(v => Some(str + h + ":" + v + "\n")).getOrElse(str)
      }
    }
  }

  def bucketHost(bucket: String) = bucket + ".s3.amazonaws.com"

  def authorization(s3key: String, s3Secret: String, req: BucketRequest): String = {
    "AWS " + s3key + ":" + sign(s3Secret, req)
  }

  def authorization(s3key: String, s3Secret: String, req: ServiceRequest): String = {
    "AWS " + s3key + ":" + sign(s3Secret, req)
  }


  private def calculateHMAC(key: String, data: String): String = {
    val signingKey = new SecretKeySpec(key.getBytes(UTF_8), ALGORITHM)
    val mac = Mac.getInstance(ALGORITHM)
    mac.init(signingKey)
    val rawHmac = mac.doFinal(data.getBytes)
    new sun.misc.BASE64Encoder().encode(rawHmac)
  }

}


case class ServiceRequest(url: String) extends S3Request {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, url);
}

object ListAllBuckets {

  val log = Logger.get("ListAllBuckets")

  def apply(s3: S3.S3Client): Future[List[String]] = {
    s3(ServiceRequest("/")).map {
      hResp =>
        if (hResp.getStatus.getCode != 200) {
          val response = new String(hResp.getContent.array(), UTF_8)
          log.error(response)
          throw new RuntimeException("Cant List Buckets")
        }
        var resp: String = hResp.getContent.toString(UTF_8)
        var xResp = XML.loadString(resp)
        ((xResp \\ "Buckets" \\ "Bucket" \\ "Name") map (_.text)).toList
    }
  }
}

trait S3Request extends HttpRequestProxy {

  def setHeaders(headers: (String, String)*) = {
    headers.foreach(h => httpRequest.setHeader(h._1, h._2))
    this
  }

  def header(name: String): Option[String] = {
    Option(httpRequest.getHeader(name))
  }

}

trait BucketRequest extends S3Request {

  def bucket: String

  def sign: Boolean = true

  val queries = new HashMap[String, String]

  def normalizeKey(key: String) = {
    if (key.startsWith("/")) key
    else "/" + key
  }

  def query(q: (String, String)*) = {
    q.foreach(kv => queries += kv._1 -> kv._2)
    this
  }
}

trait ObjectRequest extends BucketRequest {
  def key: String
}

case class Put(bucket: String, key: String, content: ChannelBuffer, headers: (String, String)*) extends ObjectRequest {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, PUT, normalizeKey(key));
  setContent(content)
  setHeader(CONTENT_LENGTH, content.readableBytes().toString)
  headers.foreach(h => setHeader(h._1, h._2))
}

case class Get(bucket: String, key: String, override val sign: Boolean = true) extends ObjectRequest {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, normalizeKey(key));
}

case class Head(bucket: String, key: String, override val sign: Boolean = true) extends ObjectRequest {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, HEAD, normalizeKey(key));
}

case class CreateBucket(bucket: String) extends BucketRequest {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, PUT, "/");
  setHeader(CONTENT_LENGTH, "0")
}

case class DeleteBucket(bucket: String) extends BucketRequest {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, DELETE, "/");
}

case class Delete(bucket: String, key: String) extends ObjectRequest {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, DELETE, normalizeKey(key));
  setHeader(CONTENT_LENGTH, "0")
}

case class ListBucket(bucket: String, marker: Option[Marker] = None, prefix: Option[Prefix] = None) extends BucketRequest {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, "/");
  marker.foreach(m => query("marker" -> m.marker))
  prefix.foreach(p => query("prefix" -> p.prefix))
}

case class Marker(marker: String)

case class Prefix(prefix: String)

case class ObjectInfo(key: String, lastModified: String, etag: String, size: String, storageClass: String, ownerId: String, ownerDisplayName: String)

object Delete {
  val success = NO_CONTENT
}

object DeleteBucket {

  val log = Logger.get(classOf[DeleteBucket])


  import com.heroku.finagle.aws.S3.S3Client;

  def deleteAllItemsInBucket(s3: S3Client, bucket: String): Future[Boolean] = {
    ListBucket.getKeysNonBlocking(s3, bucket).flatMap {
      keys =>
        Future.collect(keys.map {
          key => {
            s3(Delete(bucket, key)).map {
              resp =>
                val success: Boolean = resp.getStatus.equals(Delete.success)
                if (!success) log.warning("failed to delete %s in %s, status %s", key, bucket, resp.getStatus)
                success
            }
          }
        }).map {
          booleans =>
            booleans.foldLeft(true)(_ && _)
        }
    }
  }
}

object ListBucket {

  import com.heroku.finagle.aws.S3.S3Client;

  val log = Logger.get(classOf[ListBucket])


  @tailrec
  def getKeys(s3: S3Client, bucket: String, marker: Option[Marker] = None, all: List[String] = List()): List[String] = {
    val (keys, truncated) = parseKeys(s3(ListBucket(bucket, marker)).get())
    log.debug("B Got %s keys for %s", keys.size.toString, bucket)
    if (truncated) {
      getKeys(s3, bucket, Some(Marker(keys.last)), keys ++ all)
    } else {
      keys ++ all
    }
  }


  def getKeysNonBlocking(s3: S3Client, bucket: String, marker: Option[Marker] = None, all: List[String] = List()): Future[List[String]] = {
    s3(ListBucket(bucket, marker)).flatMap {
      hResp =>
        val (keys, truncated) = parseKeys(hResp)
        log.debug("NB Got %s keys for %s", keys.size.toString, bucket)
        if (truncated) {
          getKeysNonBlocking(s3, bucket, Some(Marker(keys.last)), keys ++ all)
        } else {
          Future.value(keys ++ all)
        }
    }
  }

  private def parseKeys(hResp: HttpResponse): (List[String], Boolean) = {
    if (hResp.getStatus != OK) throw new IllegalStateException("Status was not OK: " + hResp.getStatus.toString)
    val resp: String = hResp.getContent.toString(UTF_8)
    val xResp = XML.loadString(resp)
    val keys = ((xResp \\ "Contents" \\ "Key") map (_.text)).toList
    val truncated = ((xResp \ "IsTruncated") map (_.text.toBoolean))
    (keys, truncated.headOption.getOrElse(false))
  }

  private def parseListing[T](hResp: HttpResponse, xform: ObjectInfo => T): (List[T], Boolean, Option[Marker]) = {
    if (hResp.getStatus != OK) throw new IllegalStateException("Status was not OK: " + hResp.getStatus.toString)
    val resp: String = hResp.getContent.toString(UTF_8)
    val xResp = XML.loadString(resp)
    val keys = ((xResp \\ "Contents")).map {
      content =>
        ObjectInfo(
          (content \ "Key").text,
          (content \ "LastModified").text,
          (content \ "ETag").text,
          (content \ "Size").text,
          (content \ "StorageClass").text,
          (content \ "Owner" \ "ID").text,
          (content \ "Owner" \ "DisplayName").text
        )
    }.toList
    val truncated = ((xResp \ "IsTruncated") map (_.text.toBoolean))
    (keys.map(xform), truncated.headOption.getOrElse(false), keys.lastOption.map(o=>Marker(o.key)))
  }

  def listAll[T](s3: S3Client, bucket: String, prefix: Option[Prefix] = None, marker: Option[Marker] = None, all: List[T] = List())(xform: ObjectInfo => T): Future[List[T]] = {
    s3(ListBucket(bucket, marker, prefix)).flatMap {
      hResp =>
        val (keys, truncated, marker) = parseListing(hResp, xform)
        log.debug("NB Got %s Objects for %s", keys.size.toString, bucket)
        if (truncated) {
          listAll(s3, bucket, prefix, marker, keys ++ all)(xform)
        } else {
          Future.value(keys ++ all)
        }
    }
  }

}

class S3Response(resp: HttpResponse) extends HttpResponseProxy {
  def httpResponse = resp
}



