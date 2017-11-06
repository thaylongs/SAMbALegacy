package br.uff.spark

import java.sql.{Connection, SQLException}
import java.util.concurrent.atomic.AtomicInteger
import com.datastax.driver.core.CodecRegistry
import br.uff.spark.database.CassandraCodecs.CodecsTaskToUUID
import com.datastax.driver.core.{Cluster, PoolingOptions, Session}

object DataSource {

  private val waitForClose = new AtomicInteger()

  def upCount(): Unit = {
    waitForClose.incrementAndGet();
  }

  def downCount() = {
    waitForClose.decrementAndGet()
  }

  def close(): Unit = {
    while (waitForClose.get() > 0) {
      Thread.sleep(200)
    }
    cluster.close()
  }


  private var cluster = createConnectionPool()
  private var session = cluster.connect("dfanalyzer")

  private def createConnectionPool(): Cluster = {
    val poolingOptions = new PoolingOptions
    val cluster = Cluster.builder()
      .addContactPoint("127.0.0.1")
      .withPort(9042)
      .withPoolingOptions(poolingOptions)
      .withCodecRegistry(CodecRegistry.DEFAULT_INSTANCE.register(new CodecsTaskToUUID))
      .build();
    cluster
  }


  @throws[SQLException]
  def getConnection: Session = {
    if (cluster.isClosed) {
      cluster = createConnectionPool()
      session = cluster.connect("dfanayler")
    }
    session
  }
}
