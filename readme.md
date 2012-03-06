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

##License

    Copyright 2001-2012 Scott Clasen <scott.clasen@gmail.com>

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

