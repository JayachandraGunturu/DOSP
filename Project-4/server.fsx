#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#r "nuget: Akka.TestKit"
#r "nuget: Akka.Serialization.Hyperion"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open System.IO;
open System.Diagnostics
open System.Collections.Generic
open Akka.Serialization
open System.Linq
open Akka.Remote

let configuration = 
    ConfigurationFactory.ParseString(
          @"akka {
            stdout-loglevel : off
            loglevel : off
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
            actor.serializers{
                wire  = ""Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion""
            }
            actor.serialization-bindings {
                ""System.Object"" = wire 
            }
            actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            remote {
                maximum-payload-bytes = 30000000 bytes
                 helios.tcp {
                    port = 9090
                    hostname = 192.168.0.98
                }
            }
        }")

let system = ActorSystem.Create("ServerEngine",configuration)
let mutable usersMap:Map<string,bool> = Map.empty
let mutable usersRefMap:Map<string,IActorRef> = Map.empty
let mutable subscribersList:Dictionary<string,List<String>> = new Dictionary<string,List<String>>()
let mutable hashtagTweets:Map<string,List<string>> = Map.empty
let mutable userMentionTweets:Map<string,List<string>> = Map.empty
let mutable allSubscribedTweets:Map<string,List<string>> = Map.empty
let timer = new Stopwatch()
let timer1 = new Stopwatch()
let timer2 = new Stopwatch()
let timer3 = new Stopwatch()
let timer4 = new Stopwatch()
let mutable tweets = 0
let mutable retweets = 0
let mutable queryHashtags = 0
let mutable queryMentions = 0
let mutable timetweets = 0
let mutable timeretweets = 0
let mutable timequeryHashtags = 0
let mutable timequeryMentions = 0


type ServerMessages = 
 |Register of String * IActorRef
 |GoOnline of String
 |GoOffline of String
 |Tweet of String * String
 |ReTweet of String * String
 |Subscribe of String * List<String>
 |Query
 |PrintUsers
 |CountSubscribers
 |Acknowledgement of String 
 |SubscribedTweet of String * String
 |SubscribedReTweet of String * String
 |HashTag of String * String * String
 |HashTagTweet of String * String * String
 |Mention of String * String * String
 |Mentioned of String * String
 |QueryMention of String
 |QueryHashTag of String * String
 |QueryTweets of String
 |QueryMentionResults of List<String>
 |QueryHashTagResults of List<String>
 |QueryTweetsResults of List<String>
 |AddNewSubscriber of String * List<String>
 |PrintSubscriberCount
 |MarkOnline of String
 |MarkOffline of String
 |MakeTweet of String * String
 |MakeReTweet of String * String
 |HashTagByUser of String * String * String
 |MentionedUserTweet of String * String * String
 |MentionQuery of String
 |HashTagQuery of String * String
 |AllTweetsQuery of String
 |PrintTime
 |Start
 |PrintStats

let timerActor (mailbox: Actor<_>) =
    let mutable requests = 0 |> int64
    let mutable n = 1 |> int64
    let noOfUsers = usersMap.Count |> int64
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |PrintTime ->   requests <- requests + int64(1)
                      //printfn "%d" requests
                      //if (requests%(int64(500) * n) = int64(0)) then
                      //if(requests = int64(100000)) then
                        timer.Stop()
                        let timeTaken = timer.ElapsedMilliseconds - (requests*noOfUsers) |>double
                        //printfn "Time taken for %d requests is %f ms" requests timeTaken
                        printfn "Actions performed so far"
                        printfn "Tweets  : %d" tweets
                        printfn "ReTweets  : %d" retweets
                        printfn "Query Hashtags  : %d" queryHashtags
                        printfn "Query Mentions  : %d" queryMentions
                        //n <- n + int64(1)
        | _ -> ()
        return! loop()
    }
    loop()

let printAllUsers (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |PrintUsers -> printfn "Printing method %d" usersMap.Count
        | _ -> ()

        return! loop()
    }
    loop()

let printPerformance (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |PrintStats -> printfn "hello"

        | _ -> ()

        return! loop()
    }
    loop()

let RegisterUser (mailbox: Actor<_>) =
    let temp = new List<String>()
    let tempList = new List<string>()
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |Register (user,actorRef) -> usersMap <- usersMap.Add(user,true)
                                     usersRefMap <- usersRefMap.Add(user,actorRef)
                                     userMentionTweets <- userMentionTweets.Add(user,tempList)
                                     allSubscribedTweets <- allSubscribedTweets.Add(user,tempList)

                                     if(usersRefMap.ContainsKey(user)) then
                                        actorRef <! Acknowledgement "Done" 
        | _ -> ()

        return! loop()
    }
    loop()

let userOnline (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |MarkOnline user -> usersMap <- usersMap.Add(user,true)
        | _ -> ()
        return! loop()
    }
    loop()

let userOffline (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |MarkOffline user -> usersMap <- usersMap.Add(user,false) 
        | _ -> ()
        return! loop()
    }
    loop()

let userTweet (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |MakeTweet (user,msg) -> timer1.Start()
                                 let subscribers = subscribersList.[user]
                                 for actorRef in subscribers do
                                    if usersMap.[actorRef] then
                                        usersRefMap.[actorRef] <! SubscribedTweet (user,msg)
                                    allSubscribedTweets.[actorRef].Add(msg)
                                 tweets <- tweets + 1
                                 timer1.Stop()
        | _ -> ()
        return! loop()
    }
    loop()

let userRetweet (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |MakeReTweet (user,msg) -> timer2.Start()
                                   let subscribers = subscribersList.[user]
                                   for actorRef in subscribers do
                                        if usersMap.[actorRef] then
                                            usersRefMap.[actorRef] <! SubscribedReTweet (user,msg)
                                        allSubscribedTweets.[actorRef].Add(msg)
                                   retweets <- retweets + 1
                                   timer2.Stop()
        | _ -> ()

        return! loop()
    }
    loop()


let userHashTag (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |HashTagByUser (user,msg,tag) -> //printfn "%s # %s" msg tag
                                  timer3.Start()
                                  if hashtagTweets.ContainsKey(tag) then
                                    let mutable tempList = hashtagTweets.[tag]
                                    tempList.Add(msg)
                                    hashtagTweets <- hashtagTweets.Add(tag,tempList)
                                  else
                                    let tempList = new List<string>()
                                    hashtagTweets <- hashtagTweets.Add(tag,tempList)
                                  
                                  let subscribers = subscribersList.[user]
                                  for actorRef in subscribers do
                                        if usersMap.[actorRef] then
                                            usersRefMap.[actorRef] <! HashTagTweet (user,msg,tag)
                                  timer3.Stop()
        | _ -> ()

        return! loop()
    }
    loop()

let userMention (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |MentionedUserTweet (msg,user,mentionedBy) ->  
                                        timer4.Start()
                                        if userMentionTweets.ContainsKey(user) then
                                            let mutable tempList = userMentionTweets.[user]
                                            tempList.Add(msg)
                                            userMentionTweets <- userMentionTweets.Add(user,tempList)
                                        else
                                            let tempList = new List<string>()
                                            userMentionTweets <- userMentionTweets.Add(user,tempList)
                                            if usersMap.[user] then
                                                usersRefMap.[user] <! Mentioned (msg,mentionedBy)
                                           //printfn "%s @ %s" msg user
                                        timer4.Stop()
        | _ -> ()

        return! loop()
    }
    loop()

let userQuery (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |MentionQuery user -> 
                            timer4.Start()
                            if usersRefMap.ContainsKey(user) && userMentionTweets.ContainsKey(user) then 
                                usersRefMap.[user] <! QueryMentionResults userMentionTweets.[user]
                            queryMentions <- queryMentions + 1
                            timer4.Stop()
        |HashTagQuery (user,hashtag) -> 
                            timer3.Start()
                            if hashtagTweets.ContainsKey(hashtag) then
                                usersRefMap.[user] <! QueryHashTagResults hashtagTweets.[hashtag]
                            queryHashtags <- queryHashtags + 1
                            timer3.Stop()
        |AllTweetsQuery user -> 
                            if usersRefMap.ContainsKey(user) && userMentionTweets.ContainsKey(user) then 
                                usersRefMap.[user] <! QueryTweetsResults allSubscribedTweets.[user] 
        | _ -> ()
        return! loop()
    }
    loop()

let subscribe (mailbox: Actor<_>) =
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |AddNewSubscriber (user,subList) -> subscribersList.Add(user,subList)

        |PrintSubscriberCount ->for i in subscribersList do
                                    printfn "here"
                                    let mutable list = ""
                                    for sub in i.Value do
                                        list <- list + sub
                                    let res = i.Key + " Subscribers :- " + list
                                    printfn "%s" res
        | _ -> ()

        return! loop()
    }
    loop()

let requestHandler (mailbox: Actor<_>) = 
        let registerActor = spawn system "RegisterUser" RegisterUser
        let userOnline = spawn system "UserIsOnline" userOnline
        let userOffline = spawn system "UserIsOffline" userOffline
        let doTweet = spawn system "DoTweet" userTweet
        let doRetweet = spawn system "DoRetweet" userRetweet
        let addSubscriber = spawn system "Subscribe" subscribe
        let addHashTag = spawn system "HashTagging" userHashTag
        let mentionUser = spawn system "UserMention" userMention
        let runQuery = spawn system "Query" userQuery
        let getUsersList = spawn system "UserStatus" printAllUsers
        let timerActor = spawn system "TimerActor" timerActor
        let statsPrinter = spawn system "stats" printPerformance
        let rec loop() =
            actor {
                let! message = mailbox.Receive()
                match message with 
                |Start -> printfn "Server up and running......"
                |Register (user,userRef) -> registerActor <! Register(user,mailbox.Sender())
                                            //printfn "%s has been registered" user  
                |GoOnline user ->  userOnline <! MarkOnline user
                |GoOffline user -> userOffline <! MarkOffline user
                |Tweet (user,msg) -> doTweet <! MakeTweet(user,msg)
                |ReTweet (user,msg) -> doRetweet <! MakeReTweet(user,msg) 
                |Subscribe(user,subList) -> addSubscriber <! AddNewSubscriber (user,subList)
                |HashTag (user,msg,tag) -> addHashTag <! HashTagByUser (user,msg,tag)
                |Mention (msg,user,mentionedBy) -> mentionUser <! MentionedUserTweet (msg,user,mentionedBy)
                |QueryMention user -> runQuery <! MentionQuery user
                |QueryHashTag (user,tag) -> runQuery <! HashTagQuery (user,tag)
                |QueryTweets user ->  runQuery <! AllTweetsQuery user
                |PrintUsers -> getUsersList <! PrintUsers
                |CountSubscribers -> addSubscriber <! PrintSubscriberCount
                | _ -> printfn "No type matched"
                timer.Start()
                //timerActor <! PrintTime
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), timerActor, PrintTime)
                //system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100000.0), statsPrinter, PrintStats)
                return! loop() 
            }
        loop()

let boss =  spawn system "requestHandler" requestHandler

boss  <! Start

system.WhenTerminated.Wait()