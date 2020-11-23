package com.fakir.samples.utils



object Encoder {
  def sha1(s:String ) = java.security.MessageDigest.getInstance("SHA-1").digest(s.getBytes).map((b: Byte) => (if (b >= 0 & b < 16) "0" else "") + (b & 0xFF).toHexString).mkString
}