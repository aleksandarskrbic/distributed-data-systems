package wal

import cats.effect._

object WalTest extends IOApp.Simple {
  override def run: IO[Unit] =
    program[IO]

  def program[F[_]: Sync]: F[Unit] =
    Sync[F].pure(println("Hello World!"))
}
