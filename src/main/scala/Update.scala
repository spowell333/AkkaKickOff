
/**
 * Copyright (C) 2009-2012 Typesafe Inc. <http://www.typesafe.com>
*/
package akka.tutorial.first.scala
   
import akka.actor._
import akka.routing.BroadcastRouter
import akka.util.Duration
import akka.util.duration._


sealed trait Instrument
case class Equity(ticker: String, isin: String, sedol: String) extends Instrument

    
object Update extends App {
         
  calculate(nrOfWorkers = 1)
            
  sealed trait UpdateMessage
  case class TickerChange(ticker: String, newTicker: String) extends UpdateMessage
  case class ISINChange(isin: String, newISIN: String) extends UpdateMessage
  case class Listing(ticker: String, isin: String, sedol: String) extends UpdateMessage
  case class Delisting(ticker: String) extends UpdateMessage
  case object ShutDown extends UpdateMessage
  case object Dump extends UpdateMessage
                       
  class Consumer extends Actor {

    var instruments = Set[Instrument]()
                                                                         
    def receive = {
      case Dump                            ⇒ instruments.map(i ⇒ println(i))
      case Delisting(ticker)    ⇒ instruments ={val DELISTED_TICKER = ticker;
                                                instruments.filter( i ⇒ i match { case Equity(DELISTED_TICKER, _, _) ⇒ false
                                                                                  case _                             ⇒ true
                                                                                } 
                                                                  )
                                               }
      case ISINChange(oi, ni)   ⇒ {
                                   val OLD_ISIN = oi; val NEW_ISIN = ni;
                                   instruments = instruments.map(i ⇒ { i match {case Equity(t, OLD_ISIN, s) ⇒ Equity(t, NEW_ISIN, s)
                                                                                case _                      ⇒ i
                                                                              } 
                                                                     }
                                                                )     
                                  }
      case TickerChange(ot, nt) ⇒ {
                                   val OLD_TICKER = ot;
                                   val NEW_TICKER = nt;
                                   instruments = instruments.map(i ⇒ { i match {case Equity(OLD_TICKER, isin, s) ⇒ Equity(NEW_TICKER, isin, s)
                                                                                case _                           ⇒ i
                                                                               }
                                                                     }
                                                                )
                                  }
            case Listing(ticker, isin, sedol) ⇒ instruments.+=(Equity(ticker, isin, sedol))
        }
    }
                                                                                                      
    class Master(nrOfWorkers: Int) extends Actor {
       val start: Long = System.currentTimeMillis
                                                                                                                              
       val refDataRouter = context.actorOf(
         Props[Consumer].withRouter(BroadcastRouter(nrOfWorkers)), 
         name = "refDataRouter"
       )

      def refDataChangeAllowed(ticker:String): Boolean ={
        ticker != "VOD"  
      }
       
       def receive = {
         case ShutDown                        ⇒ context.system.shutdown()
         case Dump                            ⇒ refDataRouter ! Dump
         case Delisting(ticker)               ⇒ refDataRouter ! Delisting(ticker)
         case Listing(ticker, isin, sedol)    ⇒ refDataRouter ! Listing(ticker, isin, sedol)
         case TickerChange(ticker, newTicker) ⇒ if(refDataChangeAllowed(ticker)) refDataRouter ! TickerChange(ticker, newTicker)
         case ISINChange(isin, newISIN)       ⇒ refDataRouter ! ISINChange(isin, newISIN)
       }
    }

   def calculate(nrOfWorkers: Int) {
      // Create an Akka system
      val system = ActorSystem("RefDataSystem")
      // create the master
      val master = system.actorOf(Props(new Master( nrOfWorkers)), name = "master")
      
      // start the calculation
      master ! Listing("VOD","GB0001","0001")
      master ! Listing("NXT","GB0002","0002")
      master ! Listing("RYL","GB0003","0003")
      master ! Listing("RDS","GB0004","0004")
      master ! Dump
      println("==================")
      master ! TickerChange("VOD", "VODa")
      master ! TickerChange("NXT", "NEXT")
      master ! Delisting("RDS")
      master ! Dump
      master ! ShutDown
     }
}
