
import com.mongodb.client.model.Filters.exists
import com.mongodb.client.model.Projections.excludeId
import org.mongodb.scala.model.Sorts.descending
import org.mongodb.scala.{Completed, Document, MongoClient, MongoCollection, Observable, Observer}

object Main extends App {
  // Use a Connection String
  val mongoClient: MongoClient = MongoClient("mongodb://localhost/test")
  val database = mongoClient.getDatabase("test")
  val collection: MongoCollection[Document] = database.getCollection("test");
  val doc: Document = Document(
    "name" -> "MongoDB",
    "type" -> "database",
    "count" -> 1,
    "info" -> Document(
      "x" -> 203,
      "y" -> 102
      )
    )
  val observable: Observable[Completed] = collection.insertOne(doc)
  observable.subscribe(new Observer[Completed] {

    override def onNext(result: Completed): Unit = println("Inserted")

    override def onError(e: Throwable): Unit = println("Failed")

    override def onComplete(): Unit = println("Completed")
  })
  val documents = (1 to 5) map { i: Int => Document("i" -> i) }
  val insertObservable: Observable[Completed] = collection.insertMany(documents)
  insertObservable.subscribe(new Observer[Completed] {

    override def onNext(result: Completed): Unit = println("Inserted")

    override def onError(e: Throwable): Unit = println("Failed")

    override def onComplete(): Unit = println("Completed")
  })
  val insertAndCount = for {
    insertResult <- insertObservable
    countResult <- collection.count()
  } yield countResult

  var findObservable = collection.find().first()
  findObservable.subscribe(new Observer[Document] {

    override def onError(e: Throwable): Unit = println("Failed")

    override def onComplete(): Unit = println("Completed")

    override def onNext(result: Document): Unit = println(result)
  })
  val findAllObservable = collection.find()
  findAllObservable.subscribe(new Observer[Document] {

    override def onError(e: Throwable): Unit = println("Failed")

    override def onComplete(): Unit = println("Completed")

    override def onNext(result: Document): Unit = println(result)
  })

  findObservable = collection.find(exists("i")).sort(descending("i")).first()
  findObservable.subscribe(new Observer[Document] {

    override def onError(e: Throwable): Unit = println("Failed")

    override def onComplete(): Unit = println("Completed")

    override def onNext(result: Document): Unit = println(result)
  })
  findObservable = collection.find().projection(excludeId()).first()
  findObservable.subscribe(new Observer[Document] {

    override def onError(e: Throwable): Unit = println("Failed")

    override def onComplete(): Unit = println("Completed")

    override def onNext(result: Document): Unit = println(result)
  })
  Thread.sleep(2000)
  mongoClient.close()
}