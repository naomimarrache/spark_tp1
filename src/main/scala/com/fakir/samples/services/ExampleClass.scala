package com.fakir.samples.services

import com.solarmosaic.client.mail._
import com.solarmosaic.client.mail.configuration._
import com.solarmosaic.client.mail.content.ContentType.MultipartTypes
import com.solarmosaic.client.mail.content._

class ExampleClass extends EnvelopeWrappers {
  val config = SmtpConfiguration("localhost", 25)
  val mailer = Mailer(config)
  val content = Multipart(
    parts = Seq(Text("text"), Html("<p>text</p>")),
    subType = MultipartTypes.alternative
  )

  val envelope = Envelope(
    from = "nao.17@hotmail.fr",
    to = Seq("nao.17@hotmail.fr"),
    subject = "test",
    content = content
  )

  mailer.send(envelope)
}
