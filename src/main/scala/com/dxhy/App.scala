package com.dxhy

import java.util.UUID

/**
 * Hello world!
 *
 */
object App extends App {
  println( "Hello World!" )
  println(UUID.randomUUID().toString().replace("-", ""))
  println(UUID.randomUUID().toString())
}
