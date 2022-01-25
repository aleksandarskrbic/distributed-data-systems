package wal.internal

import cats.effect._
import cats.effect.std.Semaphore
import cats.implicits._

import java.io.RandomAccessFile
import java.nio.file.{ Files, Path, Paths }

sealed trait Result
final case class ReadResult(content: Array[Byte])            extends Result
final case class AppendResult(written: Long, position: Long) extends Result

sealed trait Store[F[_]] {
  def read(position: Long): F[ReadResult]
  def append(content: Array[Byte]): F[AppendResult]
}

final class DiskStore[F[_]](
  file: RandomAccessFile,
  sizeRef: Ref[F, Long],
  semaphore: Semaphore[F],
  lenWidth: Int = 8
)(implicit F: Sync[F])
    extends Store[F] {

  override def read(position: Long): F[ReadResult] =
    for {
      _    <- semaphore.acquire
      size <- readSize(position)
      res  <- readAt(Array.ofDim[Byte](size.toInt), position + lenWidth)
      _    <- semaphore.release
    } yield ReadResult(res)

  override def append(content: Array[Byte]): F[AppendResult] =
    for {
      _        <- semaphore.acquire
      position <- sizeRef.get
      size      = content.length.toLong
      _        <- writeRecordLength(size)
      _        <- writeRecord(content)
      written   = size + lenWidth
      _        <- sizeRef.update(_ + written)
      _        <- semaphore.release
    } yield AppendResult(written, position)

  private def writeRecordLength(length: Long): F[Unit] =
    F.delay(file.writeLong(length))

  private def writeRecord(content: Array[Byte]): F[Unit] =
    F.delay(file.write(content))

  private def readSize(position: Long): F[Long] =
    F.delay {
      file.seek(position)
      file.readLong()
    }

  private def readAt(byteArray: Array[Byte], offset: Long): F[Array[Byte]] =
    F.delay {
      file.seek(offset)
      file.readFully(byteArray)
      byteArray
    }
}

object Store {
  def make[F[_]](filename: String)(implicit F: Async[F]): F[Store[F]] =
    for {
      semaphore <- Semaphore[F](1)
      path      <- F.delay(Paths.get(filename))
      file      <- F.ifM(F.delay(Files.exists(path)))(createFromPath(path), createIfNotExists(path))
      ref       <- Ref.of[F, Long](file.length)
    } yield new DiskStore[F](file, ref, semaphore)

  def make2[F[_]: Async](filename: String): F[Store[F]] =
    for {
      semaphore <- Semaphore[F](1)
      path      <- Sync[F].delay(Paths.get(filename))

      condition = Sync[F].delay(Files.exists(path))

      file <- Sync[F].ifM(condition)(createFromPath(path), createIfNotExists(path))
      ref  <- Ref.of[F, Long](file.length)
    } yield new DiskStore[F](file, ref, semaphore)

  private def createFromPath[F[_]: Sync](path: Path): F[RandomAccessFile] =
    Sync[F].delay(new RandomAccessFile(path.toString, "rw"))

  private def createIfNotExists[F[_]: Sync](path: Path): F[RandomAccessFile] =
    Sync[F].delay(Files.createFile(path)).flatMap(createFromPath(_))
}
