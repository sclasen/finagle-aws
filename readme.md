#finagle-aws

A finagle module for making life a little easier to talk to AWS via Finagle.

        import com.heroku.finagle.aws.{Get, Put, S3}
        import com.twitter.finagle.builder.ClientBuilder
        import com.twitter.util.Future
        import org.jboss.netty.buffer.ChannelBuffers
        import org.jboss.netty.handler.codec.http.HttpResponse
        import org.jboss.netty.util.CharsetUtil
        import util.Properties

        class Readme {

          val client = ClientBuilder().hosts("s3.amazonaws.com:80")
            .hostConnectionLimit(1).name("testClient")
            .codec(S3.get(Properties.envOrNone("S3_KEY").get, Properties.envOrNone("S3_SECRET").get))
            .build()

          val buffer = ChannelBuffers.wrappedBuffer("SOME BYTES".getBytes(CharsetUtil.UTF_8))

          val put: Future[HttpResponse] = client(Put("theBucket", "theKey", buffer))

          val get: Future[HttpResponse] = client(Get("theBucket", "theKey"))


        }
