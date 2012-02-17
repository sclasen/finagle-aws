package com.heroku.finagle.aws

import com.twitter.finagle.http.Http
import com.twitter.finagle.http.netty.{HttpResponseProxy, HttpRequestProxy}
import org.jboss.netty.channel._
import org.joda.time.format.DateTimeFormat
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import org.jboss.netty.handler.codec.http.HttpVersion._
import com.twitter.finagle._
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
import com.twitter.util.Future
import annotation.tailrec

object S3 {
  type S3Client = Service[S3Request, HttpResponse]

  def get(key: String, secret: String) = new S3(key, secret)
}

case class S3(private val key: String, private val secret: String, httpFactory: CodecFactory[HttpRequest, HttpResponse] = Http.get()) extends CodecFactory[S3Request, HttpResponse] {

  def client = Function.const {
    new Codec[S3Request, HttpResponse] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = httpFactory.client(null).pipelineFactory.getPipeline()
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
      request.getMethod.getName,
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
    val rawHmac = mac.doFinal(data.getBytes())
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
  httpRequest.setContent(content)
  httpRequest.setHeader(CONTENT_LENGTH, content.readableBytes().toString)
  headers.foreach(h => httpRequest.setHeader(h._1, h._2))
}

case class Get(bucket: String, key: String) extends S3Request {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, normalizeKey(key));
}

case class CreateBucket(bucket: String) extends S3Request {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, PUT, "/");
  httpRequest.setHeader(CONTENT_LENGTH, "0")
}

case class Delete(bucket: String, key: String) extends S3Request {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, DELETE, normalizeKey(key));
  httpRequest.setHeader(CONTENT_LENGTH, "0")
}

case class ListBucket(bucket: String, marker: Option[String] = None) extends S3Request {
  override val httpRequest: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, "/");
  marker.foreach(m => query("marker" -> m))
}

object ListBucket {

  import com.heroku.finagle.aws.S3.S3Client;

  val log = Logger.get(classOf[ListBucket])

  def getKeys(s3: S3Client, bucket: String): List[String] = {
    getKeysB(s3, bucket)
  }

  @tailrec
  def getKeysB(s3: S3Client, bucket: String, marker: Option[String] = None, all: List[String] = List()): List[String] = {
    var hResp: HttpResponse = s3(ListBucket(bucket, marker)).get()
    var resp: String = hResp.getContent.toString(UTF_8)
    var xResp = XML.loadString(resp)
    val keys = ((xResp \\ "Contents" \\ "Key") map (_.text)).toList
    val truncated = ((xResp \ "IsTruncated") map (_.text.toBoolean))
    //if this is a deep recursion these take up a lot of space so we null out
    hResp = null
    resp = null
    xResp = null
    log.debug("Got %s keys for %s", keys.size.toString, bucket)
    if (truncated.headOption.getOrElse(false)) {
      getKeysB(s3, bucket, Some(keys.last), keys ++ all)
    } else {
      keys ++ all
    }
  }


  def getKeysNB(s3: S3Client, bucket: String, marker: Option[String] = None, all: List[String] = List()): Future[List[String]] = {
    s3(ListBucket(bucket, marker)).flatMap {
      hResp =>
        var resp: String = hResp.getContent.toString(UTF_8)
        var xResp = XML.loadString(resp)
        val keys = ((xResp \\ "Contents" \\ "Key") map (_.text)).toList
        val truncated = ((xResp \ "IsTruncated") map (_.text.toBoolean))
        //if this is a deep recursion these take up a lot of space so we null out
        resp = null
        xResp = null
        log.debug("Got %s keys for %s", keys.size.toString, bucket)
        if (truncated.headOption.getOrElse(false)) {
          getKeysNB(s3, bucket, Some(keys.last), keys ++ all)
        } else {
          Future.value(keys ++ all)
        }
    }

  }


}

class S3Response(resp: HttpResponse) extends HttpResponseProxy {
  def httpResponse = resp
}



