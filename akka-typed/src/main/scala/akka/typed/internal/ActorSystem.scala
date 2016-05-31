/**
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com/>
 */
package akka.typed
package internal

import com.typesafe.config.Config
import scala.concurrent.ExecutionContext
import java.util.concurrent.ThreadFactory
import scala.concurrent.{ ExecutionContextExecutor, Future }
import akka.{ actor => a, dispatch => d, event => e }
import scala.util.control.NonFatal
import scala.util.control.ControlThrowable
import scala.collection.immutable
import akka.typed.Dispatchers

/*
 * Actor Ideas:

  •  implement running status as integer that counts activations
  •  this counter could hold the number of messages
  •  fold everything into the ActorCell, just one type of message queue
  •  ActorCell becomes much simpler: just new(), register in children list, then attach to dispatcher with Create message enqueued
  •  ChildrenContainer can be removed, no restart stats or reserved names
  •  no more UnstartedCell
  •  still multiple kinds of ActorRef (local, remote, synchronous)
  •  ActorCell is Runnable, Dispatcher goes away (=> Executor or ExecutionContext)
  •  no more ActorRefProvider
  •  remoting/clustering is just another set of actors/extensions
  •  ActorSystem[T] can host also untyped Actors via a wrapper, but not the other way around

Receptionist:

  •  should be a new kind of Extension (where lookup yields ActorRef)
  •  obtaining a reference may either give a single remote one or a dynamic local proxy that routes to available instances—distinguished using a “stableDestination” flag (for read-your-writes semantics)
  •  perhaps fold sharding into this: how message routing is done should not matter

Streams:

  •  make new implementation of ActorMaterializer that leverages Envelope removal
  •  all internal actor creation must be asynchronous
  •  could offer ActorSystem extension for materializer
  •  remove downcasts to ActorMaterializer in akka-stream package—replace by proper function passing or Materializer APIs where needed (should make Gearpump happier as well)
  •  add new Sink/Source for ActorRef[]

Distributed Data:

  •  create new Behaviors around the logic

 *
 */

private[typed] class ActorSystemImpl[T](override val name: String,
                                        _config: Config,
                                        _cl: ClassLoader,
                                        _ec: Option[ExecutionContext],
                                        _p: Props[T])
    extends ActorRef[T](a.RootActorPath(a.Address("akka", name)) / "user") with ActorSystem[T] with ScalaActorRef[T] {

  if (!name.matches("""^[a-zA-Z0-9][a-zA-Z0-9-_]*$"""))
    throw new IllegalArgumentException(
      "invalid ActorSystem name [" + name +
        "], must contain only word characters (i.e. [a-zA-Z0-9] plus non-leading '-' or '_')")

  import a.ActorSystem.Settings
  override val settings: Settings = new Settings(_cl, _config, name)

  override def logConfiguration(): Unit = log.info(settings.toString)

  protected def uncaughtExceptionHandler: Thread.UncaughtExceptionHandler =
    new Thread.UncaughtExceptionHandler() {
      def uncaughtException(thread: Thread, cause: Throwable): Unit = {
        cause match {
          case NonFatal(_) | _: InterruptedException | _: NotImplementedError | _: ControlThrowable ⇒ log.error(cause, "Uncaught error from thread [{}]", thread.getName)
          case _ ⇒
            if (settings.JvmExitOnFatalError) {
              try {
                log.error(cause, "Uncaught error from thread [{}] shutting down JVM since 'akka.jvm-exit-on-fatal-error' is enabled", thread.getName)
                import System.err
                err.print("Uncaught error from thread [")
                err.print(thread.getName)
                err.print("] shutting down JVM since 'akka.jvm-exit-on-fatal-error' is enabled for ActorSystem[")
                err.print(name)
                err.println("]")
                cause.printStackTrace(System.err)
                System.err.flush()
              } finally {
                System.exit(-1)
              }
            } else {
              log.error(cause, "Uncaught fatal error from thread [{}] shutting down ActorSystem [{}]", thread.getName, name)
              terminate()
            }
        }
      }
    }

  override val threadFactory: d.MonitorableThreadFactory =
    d.MonitorableThreadFactory(name, settings.Daemonicity, Option(_cl), uncaughtExceptionHandler)

  override val dynamicAccess: a.DynamicAccess = new a.ReflectiveDynamicAccess(_cl)

  // this provides basic logging (to stdout) until .start() is called below
  override val eventStream = new e.EventStream((p, n) => systemActorOf(p, n), settings.DebugEventStream)
  eventStream.startStdoutLogger(settings)

  override val logFilter: e.LoggingFilter = {
    val arguments = Vector(classOf[Settings] -> settings, classOf[e.EventStream] -> eventStream)
    dynamicAccess.createInstanceFor[e.LoggingFilter](settings.LoggingFilter, arguments).get
  }

  override val log: e.LoggingAdapter = new e.BusLogging(eventStream, getClass.getName + "(" + name + ")", this.getClass, logFilter)

  /**
   * Create the scheduler service. This one needs one special behavior: if
   * Closeable, it MUST execute all outstanding tasks upon .close() in order
   * to properly shutdown all dispatchers.
   *
   * Furthermore, this timer service MUST throw IllegalStateException if it
   * cannot schedule a task. Once scheduled, the task MUST be executed. If
   * executed upon close(), the task may execute before its timeout.
   */
  protected def createScheduler(): a.Scheduler =
    dynamicAccess.createInstanceFor[a.Scheduler](settings.SchedulerClass, immutable.Seq(
      classOf[Config] -> settings.config,
      classOf[e.LoggingAdapter] -> log,
      classOf[ThreadFactory] -> threadFactory.withName(threadFactory.name + "-scheduler"))).get

  override val scheduler: a.Scheduler = createScheduler()

  override val dispatchers: Dispatchers = new Dispatchers(settings, log)
  override val executionContext: ExecutionContextExecutor = dispatchers.lookup(DispatcherDefault)

  override val startTime: Long = System.currentTimeMillis()
  override def uptime: Long = System.currentTimeMillis() - startTime

  override def terminate(): Future[Terminated] = ???
  override def whenTerminated: Future[Terminated] = ???

  override def deadLetters[U]: ActorRefImpl[U] = ???

  override def tell(msg: T): Unit = ???

  def systemActorOf(props: a.Props, name: String): Future[a.ActorRef] = ???
  def systemActorOf[U](props: Props[U], name: String): Future[ActorRef[U]] = ???
}
