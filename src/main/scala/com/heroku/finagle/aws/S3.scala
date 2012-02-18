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
import xml.XML
import org.jboss.netty.util.CharsetUtil._
import collection.mutable.HashMap
import annotation.{implicitNotFound, tailrec}
import com.twitter.util.{StorageUnit, Future}
import com.twitter.conversions.storage._

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
          pipeline.addLast("requestEncodeer", new RequestEncoder(key, secret))
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
      case s3Request: S3Request =>
        prepare(s3Request)
        ctx.sendDownstream(e)
      case unknown =>
        ctx.sendDownstream(e)
    }
  }

  def prepare(req: S3Request) {
    req.setHeaders(HOST -> bucketHost(req.bucket), DATE -> amzDate)
    req.setHeaders(AUTHORIZATION -> authorization(key, secret, req, req.bucket))
    //Add query params after signing
    if (req.queries.size > 0)
      req.setUri(req.getUri + "?" + req.queries.map(qp => (qp._1 + "=" + qp._2)).reduceLeft(_ + "&" + _))
  }

  /*DateTime format required by AWS*/
  lazy val amzFormat = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss z").withLocale(java.util.Locale.US).withZone(DateTimeZone.forOffsetHours(0))

  /*headers used by this app that need to be used in signing*/
  val ACL = "x-amz-acl"
  val STORAGE_CLASS = "x-amz-storage-class"
  val VERSION = "x-amz-version-id"
  //headers need to be in alphabetical order in this list
  val AMZN_HEADERS = List(ACL, STORAGE_CLASS, VERSION)

  val RRS = "REDUCED_REDUNDANCY"
  val ALGORITHM = "HmacSHA1"


  def amzDate: String = amzFormat.print(new DateTime)

  /*request signing for amazon*/
  /*Create the Authorization payload and sign it with the AWS secret*/
  def sign(secret: String, request: S3Request, bucket: String): String = {
    val data = List(
      request.getMethod().getName,
      request.header(CONTENT_MD5).getOrElse(""),
      request.header(CONTENT_TYPE).getOrElse(""),
      request.getHeader(DATE)
    ).foldLeft("")(_ + _ + "\n") + normalizeAmzHeaders(request) + "/" + bucket + request.getUri
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

  def authorization(s3key: String, s3Secret: String, req: S3Request, bucket: String): String = {
    "AWS " + s3key + ":" + sign(s3Secret, req, bucket)
  }

  private def calculateHMAC(key: String, data: String): String = {
    val signingKey = new SecretKeySpec(key.getBytes(UTF_8), ALGORITHM)
    val mac = Mac.getInstance(ALGORITHM)
    mac.init(signingKey)
    val rawHmac = mac.doFinal(data.getBytes)
    new sun.misc.BASE64Encoder().encode(rawHmac)
  }

}


trait S3Request extends HttpRequestProxy {

  def bucket: String

  val queries = new HashMap[String, String]

  def setHeaders(headers: (String, String)*) = {
    headers.foreach(h => httpRequest.setHeader(h._1, h._2))
    this
  }

  def header(name: String): Option[String] = {
    Option(httpRequest.getHeader(name))
  }

  def normalizeKey(key: String) = {
    if (key.startsWith("/")) key
    else "/" + key
  }

  def query(q: (String, String)*) = {
    q.foreach(kv => queries += kv._1 -> kv._2)
    this
  }
}

case class Put(bucket: String, key: String, content: ChannelBuffer, headers: (String, String)*) extends S3Request {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, PUT, normalizeKey(key));
  setContent(content)
  setHeader(CONTENT_LENGTH, content.readableBytes().toString)
  headers.foreach(h => setHeader(h._1, h._2))
}

case class Get(bucket: String, key: String) extends S3Request {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, normalizeKey(key));
}

case class CreateBucket(bucket: String) extends S3Request {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, PUT, "/");
  setHeader(CONTENT_LENGTH, "0")
}

case class DeleteBucket(bucket: String) extends S3Request {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, DELETE, "/");
}

case class Delete(bucket: String, key: String) extends S3Request {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, DELETE, normalizeKey(key));
  setHeader(CONTENT_LENGTH, "0")
}

case class ListBucket(bucket: String, marker: Option[String] = None) extends S3Request {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, "/");
  marker.foreach(m => query("marker" -> m))
}


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
  def getKeys(s3: S3Client, bucket: String, marker: Option[String] = None, all: List[String] = List()): List[String] = {
    val (keys, truncated) = parseKeys(s3(ListBucket(bucket, marker)).get())
    log.debug("B Got %s keys for %s", keys.size.toString, bucket)
    if (truncated) {
      getKeys(s3, bucket, Some(keys.last), keys ++ all)
    } else {
      keys ++ all
    }
  }


  def getKeysNonBlocking(s3: S3Client, bucket: String, marker: Option[String] = None, all: List[String] = List()): Future[List[String]] = {
    s3(ListBucket(bucket, marker)).flatMap {
      hResp =>
        val (keys, truncated) = parseKeys(hResp)
        log.debug("NB Got %s keys for %s", keys.size.toString, bucket)
        if (truncated) {
          getKeysNonBlocking(s3, bucket, Some(keys.last), keys ++ all)
        } else {
          Future.value(keys ++ all)
        }
    }

  }

  private def parseKeys(hResp: HttpResponse): (List[String], Boolean) = {
    if (hResp.getStatus != OK) throw new IllegalStateException("Status was not OK: " + hResp.getStatus.toString)
    var resp: String = hResp.getContent.toString(UTF_8)
    var xResp = XML.loadString(resp)
    val keys = ((xResp \\ "Contents" \\ "Key") map (_.text)).toList
    val truncated = ((xResp \ "IsTruncated") map (_.text.toBoolean))
    (keys, truncated.headOption.getOrElse(false))
  }

}

class S3Response(resp: HttpResponse) extends HttpResponseProxy {
  def httpResponse = resp
}



