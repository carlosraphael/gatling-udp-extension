package io.gatling.udp

import org.jboss.netty.channel.Channel
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import io.gatling.core.akka.BaseActor
import io.gatling.core.check.CheckResult
import io.gatling.core.result.writer.DataWriterClient
import io.gatling.core.session.Session
import io.gatling.core.util.TimeHelper.nowMillis
import io.gatling.core.result.writer.DataWriter
import scala.collection.mutable
import io.gatling.core.validation.Failure
import io.gatling.core.check.Check
import io.gatling.core.util.TimeHelper._
import io.gatling.core.result.message.{ Status, KO, OK }
import java.util.concurrent.Executors
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory
import org.jboss.netty.bootstrap.ConnectionlessBootstrap
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.Channels
import java.net.InetSocketAddress
import org.jboss.netty.channel.FixedReceiveBufferSizePredictorFactory
import org.jboss.netty.handler.timeout.WriteTimeoutHandler
import org.jboss.netty.handler.timeout.ReadTimeoutHandler
import org.jboss.netty.util.HashedWheelTimer

/**
 * @author carlos.raphael.lopes@gmail.com
 */
class UdpTrackerActor extends BaseActor with DataWriterClient {
  
  val nioThreadPool = Executors.newCachedThreadPool
  val nioDatagramChannelFactory = new NioDatagramChannelFactory(nioThreadPool, Runtime.getRuntime.availableProcessors())
  
  // messages to be tracked through this HashMap - note it is a mutable hashmap
  val sentMessages = mutable.HashMap.empty[String, (Long, UdpCheck, Session, ActorRef, String)]
  
  override def postStop(): Unit = {
    nioDatagramChannelFactory.releaseExternalResources()
    nioThreadPool.shutdownNow()
    sentMessages.clear()
  }
  
  override def receive: Receive = initialState
  
  val initialState: Receive = {
    case Connect(tx) => {
      val connectionBootstrap = new ConnectionlessBootstrap(nioDatagramChannelFactory)
      connectionBootstrap.setOption("sendBufferSize", 5242880)
      connectionBootstrap.setOption("receiveBufferSize", 5242880)
      connectionBootstrap.setOption("receiveBufferSizePredictorFactory", new FixedReceiveBufferSizePredictorFactory(1024))
      
      connectionBootstrap.setPipelineFactory(new ChannelPipelineFactory {
          override def getPipeline: ChannelPipeline = {
            val pipeline = Channels.pipeline()
            tx.protocol.handlers.zipWithIndex.foreach{ case (handler, i) => {
                pipeline.addLast("ChannelHandler" + i, handler)}          
            }
            pipeline.addLast("handler", new MessageListener(tx, self))
            pipeline
          }
      })
      
      val future = connectionBootstrap.connect(new InetSocketAddress(tx.protocol.address, tx.protocol.port)).awaitUninterruptibly()
      
      if (future.isSuccess) {
        val newSession = tx.session.set("udpTracker", self)
        val newTx = tx.copy(session = newSession)
        val channel = future.getChannel
        
        context.become(connectedState(channel, connectionBootstrap, newTx))
        newTx.next ! newSession
      } else {
        throw new RuntimeException
      }
    }
  }
  
  def connectedState(channel: Channel, connectionBootstrap: ConnectionlessBootstrap, tx: UdpTx): Receive = {
    def failPendingCheck(udpCheck: UdpCheck, tx: UdpTx, message: String, sentTime: Long, receivedTime: Long): UdpTx = {
      udpCheck match {
        case c: UdpCheck =>
          logRequest(tx.session, tx.requestName, KO, sentTime, receivedTime, Some(message))
          val newTx = tx.copy(updates = Session.MarkAsFailedUpdate :: tx.updates)
          
          context.become(connectedState(channel, connectionBootstrap, newTx))

          // release blocked session
          newTx.next ! newTx.session
          
          newTx
        case _ => tx
      }
    }
    
    def succeedPendingCheck(udpCheck: UdpCheck, checkResult: CheckResult, sentTime: Long, receivedTime: Long) = {
      udpCheck match {
        case check: UdpCheck =>
          logRequest(tx.session, tx.requestName, OK, tx.start, nowMillis)
          
          val newUpdates = if (checkResult.hasUpdate) {
            checkResult.update.getOrElse(Session.Identity) :: tx.updates
          } else {
            tx.updates
          }
          // apply updates and release blocked flow
          val newSession = tx.session.update(newUpdates)

          val newTx = tx.copy(session = newSession, updates = Nil)
          context.become(connectedState(channel, connectionBootstrap, newTx))
          newTx.next ! newSession
        case _ =>
      }
    }
    
    {
      // message was sent; add the timestamps to the map
      case MessageSend(requestName, check, message, session, next) =>
        val sendMessage = message.getOrElse(session("udpMessage").validate[UdpMessage].get)
        val start = nowMillis

        channel.write(sendMessage.content)
        
        tx.requestName = requestName
        val sentMessage = (start, check, session, next, requestName)
        sentMessages += session.userId -> sentMessage

        val newSession = session.set("sentMessage", sentMessage)
        val newTx = tx.copy(session = newSession, next = next)
        context.become(connectedState(channel, connectionBootstrap, newTx))
        
        check match {
          case c: UdpCheck =>
            // do this immediately instead of self sending a Listen message so that other messages don't get a chance to be handled before
            setCheck(newTx, channel, requestName, c, next, newSession, connectionBootstrap, start)
          case _ => next ! session
        }
        
        
      // message was received; publish to the datawriter and remove from the hashmap
      case MessageReceived(requestId, received, message) =>
        if (tx.session.userId.equals(requestId)) {
        	Option(tx.session("sentMessage").validate[(Long, UdpCheck, Session, ActorRef, String)].get) match {
        	case Some((startSend, check, session, next, requestName)) =>
          	logger.debug(s"Received text message on  :$message")
          	
          	implicit val cache = scala.collection.mutable.Map.empty[Any, Any]
          			
      			check.check(message, tx.session) match {
        			case io.gatling.core.validation.Success(result) =>
        			succeedPendingCheck(check, result, startSend, received)
        			
        			case s => failPendingCheck(check, tx, s"check failed $s", startSend, received)
          	}
        	  sentMessages -= requestId
        			
        	case None =>
        	}
        }
      
      case CheckTimeout(check, sentTime) =>
        check match {
          case timeout: UdpCheck =>
            failPendingCheck(check, tx, "Gatling check failed: Timeout", sentTime, nowMillis)
          case _ =>
        }
        
      case Disconnect(requestName, next, session) => {
        logger.debug(s"Disconnect channel for session: $session")
        channel.close().awaitUninterruptibly()
        connectionBootstrap.releaseExternalResources()

        next ! session
      }
    }
  }
  
  private def logRequest(session: Session, requestName: String, status: Status, started: Long, ended: Long, errorMessage: Option[String] = None): Unit = {
    writeRequestData(
      session,
      requestName,
      started,
      ended,
      ended,
      ended,
      status,
      errorMessage)
  }   
  
  def setCheck(tx: UdpTx, channel: Channel, requestName: String, check: UdpCheck, next: ActorRef, 
      session: Session, connectionBootstrap: ConnectionlessBootstrap, sentTime: Long): Unit = {

    logger.debug(s"setCheck timeout=${check.timeout}")

    // schedule timeout
    scheduler.scheduleOnce(check.timeout) {
      sentMessages.get(session.userId) match {
        case Some((startSend, check, session, next, requestName)) =>
          sentMessages -= session.userId
          self ! CheckTimeout(check, sentTime)
        case None =>
      }
    }

    val newTx = tx
      .applyUpdates(session)
      .copy(start = nowMillis, next = next, requestName = requestName)
    context.become(connectedState(channel, connectionBootstrap, newTx))
  }
}
