package wal.internal

import cats.effect._
import munit.CatsEffectSuite

class StoreTest extends CatsEffectSuite {

  val lenWidth   = 8L
  val entry      = "First Log Entry"
  val entryBytes = entry.getBytes
  val width      = entryBytes.length + lenWidth

  def createStore(filename: String) = Store.make[IO](filename)

  test("append") {
    for {
      store   <- createStore("append-test-store")
      result1 <- store.append(entryBytes)
      result2 <- store.append(entryBytes)
      result3 <- store.append(entryBytes)
    } yield {
      assertEquals(result1.written + result1.position, 1 * width)
      assertEquals(result2.written + result2.position, 2 * width)
      assertEquals(result3.written + result3.position, 3 * width)
    }
  }

  test("read") {
    for {
      store <- createStore("read-test-store")

      _ <- store.append(entryBytes)
      _ <- store.append(entryBytes)
      _ <- store.append(entryBytes)

      result1 <- store.read(0)
      result2 <- store.read(1 * width)
      result3 <- store.read(2 * width)
    } yield {
      assertEquals(entry, new String(result1.content))
      assertEquals(entry, new String(result2.content))
      assertEquals(entry, new String(result3.content))
    }
  }
}
