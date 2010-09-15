/**
 *   Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

package se.scalablesolutions.akka.dispatch

import se.scalablesolutions.akka.actor.{Actor, ActorRef}
import se.scalablesolutions.akka.config.Config.config
import net.lag.configgy.ConfigMap
import java.util.concurrent.ThreadPoolExecutor.{AbortPolicy, CallerRunsPolicy, DiscardOldestPolicy, DiscardPolicy}
import java.util.concurrent.TimeUnit
import se.scalablesolutions.akka.util.{Duration, Logging, UUID}

/**
 * Scala API. Dispatcher factory.
 * <p/>
 * Example usage:
 * <pre/>
 *   val dispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("name")
 *   dispatcher
 *     .withNewThreadPoolWithBoundedBlockingQueue(100)
 *     .setCorePoolSize(16)
 *     .setMaxPoolSize(128)
 *     .setKeepAliveTimeInMillis(60000)
 *     .setRejectionPolicy(new CallerRunsPolicy)
 *     .buildThreadPool
 * </pre>
 * <p/>
 * Java API. Dispatcher factory.
 * <p/>
 * Example usage:
 * <pre/>
 *   MessageDispatcher dispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("name");
 *   dispatcher
 *     .withNewThreadPoolWithBoundedBlockingQueue(100)
 *     .setCorePoolSize(16)
 *     .setMaxPoolSize(128)
 *     .setKeepAliveTimeInMillis(60000)
 *     .setRejectionPolicy(new CallerRunsPolicy)
 *     .buildThreadPool();
 * </pre>
 * <p/>
 *
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
object Dispatchers extends Logging {
  val THROUGHPUT             = config.getInt("akka.actor.throughput", 5)
  val THROUGHPUT_DEADLINE_MS = config.getInt("akka.actor.throughput-deadline-ms",-1)
  val MAILBOX_CAPACITY       = config.getInt("akka.actor.default-dispatcher.mailbox-capacity", -1)
  val MAILBOX_CONFIG         = MailboxConfig(
    capacity = Dispatchers.MAILBOX_CAPACITY,
    pushTimeOut = config.getInt("akka.actor.default-dispatcher.mailbox-push-timeout-ms").map(Duration(_,TimeUnit.MILLISECONDS)),
    blockingDequeue = false
  )

  lazy val defaultGlobalDispatcher = {
    config.getConfigMap("akka.actor.default-dispatcher").flatMap(from).getOrElse(globalExecutorBasedEventDrivenDispatcher)
  }

  object globalHawtDispatcher extends HawtDispatcher

  object globalExecutorBasedEventDrivenDispatcher extends ExecutorBasedEventDrivenDispatcher("global",THROUGHPUT,THROUGHPUT_DEADLINE_MS,MAILBOX_CONFIG) {
    override def register(actor: ActorRef) = {
      if (isShutdown) init
      super.register(actor)
    }
  }

  /**
   * Creates an event-driven dispatcher based on the excellent HawtDispatch library.
   * <p/>
   * Can be beneficial to use the <code>HawtDispatcher.pin(self)</code> to "pin" an actor to a specific thread.
   * <p/>
   * See the ScalaDoc for the {@link se.scalablesolutions.akka.dispatch.HawtDispatcher} for details.
   */
  def newHawtDispatcher(aggregate: Boolean) = new HawtDispatcher(aggregate)

  /**
   * Creates an thread based dispatcher serving a single actor through the same single thread.
   * Uses the default timeout
   * <p/>
   * E.g. each actor consumes its own thread.
   */
  def newThreadBasedDispatcher(actor: ActorRef) = new ThreadBasedDispatcher(actor)

  /**
   * Creates an thread based dispatcher serving a single actor through the same single thread.
   * Uses the default timeout
   * <p/>
   * E.g. each actor consumes its own thread.
   */
  def newThreadBasedDispatcher(actor: ActorRef, mailboxCapacity: Int) = new ThreadBasedDispatcher(actor, mailboxCapacity)

  /**
   * Creates an thread based dispatcher serving a single actor through the same single thread.
   * <p/>
   * E.g. each actor consumes its own thread.
   */
  def newThreadBasedDispatcher(actor: ActorRef, mailboxCapacity: Int, pushTimeOut: Duration) = new ThreadBasedDispatcher(actor, MailboxConfig(mailboxCapacity,Option(pushTimeOut),true))

  /**
   * Creates a executor-based event-driven dispatcher serving multiple (millions) of actors through a thread pool.
   * <p/>
   * Has a fluent builder interface for configuring its semantics.
   */
  def newExecutorBasedEventDrivenDispatcher(name: String) = new ExecutorBasedEventDrivenDispatcher(name, THROUGHPUT)

  /**
   * Creates a executor-based event-driven dispatcher serving multiple (millions) of actors through a thread pool.
   * <p/>
   * Has a fluent builder interface for configuring its semantics.
   */
  def newExecutorBasedEventDrivenDispatcher(name: String, throughput: Int) = new ExecutorBasedEventDrivenDispatcher(name, throughput)

  /**
   * Creates a executor-based event-driven dispatcher serving multiple (millions) of actors through a thread pool.
   * <p/>
   * Has a fluent builder interface for configuring its semantics.
   */
  def newExecutorBasedEventDrivenDispatcher(name: String, throughput: Int, throughputDeadlineMs: Int, mailboxCapacity: Int) = new ExecutorBasedEventDrivenDispatcher(name, throughput, throughputDeadlineMs, mailboxCapacity)

  /**
   * Creates a executor-based event-driven dispatcher serving multiple (millions) of actors through a thread pool.
   * <p/>
   * Has a fluent builder interface for configuring its semantics.
   */
  def newExecutorBasedEventDrivenDispatcher(name: String, throughput: Int, throughputDeadlineMs: Int, mailboxCapacity: Int, pushTimeOut: Duration) = new ExecutorBasedEventDrivenDispatcher(name, throughput, throughputDeadlineMs, MailboxConfig(mailboxCapacity,Some(pushTimeOut),false))


  /**
   * Creates a executor-based event-driven dispatcher with work stealing (TODO: better doc) serving multiple (millions) of actors through a thread pool.
   * <p/>
   * Has a fluent builder interface for configuring its semantics.
   */
  def newExecutorBasedEventDrivenWorkStealingDispatcher(name: String) = new ExecutorBasedEventDrivenWorkStealingDispatcher(name)

  /**
   * Creates a executor-based event-driven dispatcher with work stealing (TODO: better doc) serving multiple (millions) of actors through a thread pool.
   * <p/>
   * Has a fluent builder interface for configuring its semantics.
   */
  def newExecutorBasedEventDrivenWorkStealingDispatcher(name: String, mailboxCapacity: Int) = new ExecutorBasedEventDrivenWorkStealingDispatcher(name, mailboxCapacity)

  /**
   * Utility function that tries to load the specified dispatcher config from the akka.conf
   * or else use the supplied default dispatcher
   */
  def fromConfig(key: String, default: => MessageDispatcher = defaultGlobalDispatcher): MessageDispatcher =
    config.getConfigMap(key).flatMap(from).getOrElse(default)

  /*
   * Creates of obtains a dispatcher from a ConfigMap according to the format below
   *
   * default-dispatcher {
   *   type = "GlobalExecutorBasedEventDriven" # Must be one of the following, all "Global*" are non-configurable
   *                               # (ExecutorBasedEventDrivenWorkStealing), ExecutorBasedEventDriven,
   *                               # Hawt, GlobalExecutorBasedEventDriven, GlobalHawt
   *   keep-alive-ms = 60000       # Keep alive time for threads
   *   core-pool-size-factor = 1.0 # No of core threads ... ceil(available processors * factor)
   *   max-pool-size-factor  = 4.0 # Max no of threads ... ceil(available processors * factor)
   *   executor-bounds = -1        # Makes the Executor bounded, -1 is unbounded
   *   allow-core-timeout = on     # Allow core threads to time out
   *   rejection-policy = "caller-runs" # abort, caller-runs, discard-oldest, discard
   *   throughput = 5              # Throughput for ExecutorBasedEventDrivenDispatcher
   *   aggregate = off             # Aggregate on/off for HawtDispatchers
   * }
   * ex: from(config.getConfigMap(identifier).get)
   *
   * Gotcha: Only configures the dispatcher if possible
   * Returns: None if "type" isn't specified in the config
   * Throws: IllegalArgumentException if the value of "type" is not valid
   */
  def from(cfg: ConfigMap): Option[MessageDispatcher] = {
    lazy val name = cfg.getString("name", UUID.newUuid.toString)

    def threadPoolConfig(b: ThreadPoolBuilder) {
      b.configureIfPossible( builder => {
        cfg.getInt("keep-alive-ms").foreach(builder.setKeepAliveTimeInMillis(_))
        cfg.getDouble("core-pool-size-factor").foreach(builder.setCorePoolSizeFromFactor(_))
        cfg.getDouble("max-pool-size-factor").foreach(builder.setMaxPoolSizeFromFactor(_))
        cfg.getInt("executor-bounds").foreach(builder.setExecutorBounds(_))
        cfg.getBool("allow-core-timeout").foreach(builder.setAllowCoreThreadTimeout(_))
        cfg.getInt("mailbox-capacity").foreach(builder.setMailboxCapacity(_))

        cfg.getString("rejection-policy").map({
          case "abort"          => new AbortPolicy()
          case "caller-runs"    => new CallerRunsPolicy()
          case "discard-oldest" => new DiscardOldestPolicy()
          case "discard"        => new DiscardPolicy()
          case x => throw new IllegalArgumentException("[%s] is not a valid rejectionPolicy!" format x)
        }).foreach(builder.setRejectionPolicy(_))
      })
    }

    lazy val mailboxBounds: MailboxConfig = {
      val capacity = cfg.getInt("mailbox-capacity",Dispatchers.MAILBOX_CAPACITY)
      val timeout  = cfg.getInt("mailbox-push-timeout-ms").map(Duration(_,TimeUnit.MILLISECONDS))
      MailboxConfig(capacity,timeout,false)
    }

    val dispatcher: Option[MessageDispatcher] = cfg.getString("type") map {
      case "ExecutorBasedEventDrivenWorkStealing" =>
        new ExecutorBasedEventDrivenWorkStealingDispatcher(name,MAILBOX_CAPACITY,threadPoolConfig)
      
      case "ExecutorBasedEventDriven" =>
        new ExecutorBasedEventDrivenDispatcher(
          name,
          cfg.getInt("throughput",THROUGHPUT),
          cfg.getInt("throughput-deadline-ms",THROUGHPUT_DEADLINE_MS),
          mailboxBounds,
          threadPoolConfig)

      case "Hawt" =>
        new HawtDispatcher(cfg.getBool("aggregate").getOrElse(true))

      case "GlobalExecutorBasedEventDriven" =>
        globalExecutorBasedEventDrivenDispatcher

      case "GlobalHawt" =>
        globalHawtDispatcher

      case unknown =>
        throw new IllegalArgumentException("Unknown dispatcher type [%s]" format unknown)
    }

    dispatcher
  }
}
