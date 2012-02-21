#finagle-aws

A module for making life a little easier when talking to AWS via Finagle.

   import com.heroku.finagle.aws.S3.{S3Secret, S3Key}
   import com.heroku.finagle.aws.{Get, Put, S3}
   import com.twitter.util.Future
   import org.jboss.netty.buffer.ChannelBuffers
   import org.jboss.netty.handler.codec.http.HttpResponse
   import org.jboss.netty.util.CharsetUtil

   class Readme {

     val s3key = S3Key("...")

     val s3Secret = S3Secret("...")

     val client = S3.client(s3key, s3Secret)

     val buffer = ChannelBuffers.wrappedBuffer("SOME BYTES".getBytes(CharsetUtil.UTF_8))

     val put: Future[HttpResponse] = client(Put("theBucket", "theKey", buffer))

     val get: Future[HttpResponse] = client(Get("theBucket", "theKey"))


   }