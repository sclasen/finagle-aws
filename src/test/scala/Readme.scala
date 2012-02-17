import com.heroku.finagle.aws.{Get, Put, S3}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.HttpResponse
import org.jboss.netty.util.CharsetUtil

class Readme {

  val s3key = "..."

  val s3Secret = "..."

  val client = ClientBuilder().hosts("s3.amazonaws.com:80")
    .hostConnectionLimit(1).name("testClient")
    .codec(S3.get(s3key, s3Secret))
    .build()

  val buffer = ChannelBuffers.wrappedBuffer("SOME BYTES".getBytes(CharsetUtil.UTF_8))

  val put: Future[HttpResponse] = client(Put("theBucket", "theKey", buffer))

  val get: Future[HttpResponse] = client(Get("theBucket", "theKey"))


}
