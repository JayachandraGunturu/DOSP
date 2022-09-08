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
open System.Text

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
                    port = 9091
                    hostname = 192.168.0.99
                }
            }
        }")

let mutable ip = string (fsi.CommandLineArgs.GetValue 1)
let mutable noOfUsers = int (string (fsi.CommandLineArgs.GetValue 2))
let startId = int(string (fsi.CommandLineArgs.GetValue 3))
let userInitial = "User"+startId.ToString()+"_"
let mutable actorPool = [||]
let mutable noOfSubscribers =[||]
let mutable actionTime = [||]

actorPool <- Array.zeroCreate(noOfUsers + 1)
noOfSubscribers <- Array.zeroCreate(noOfUsers + 1)
actionTime <- Array.zeroCreate(noOfUsers + 1)

let system = ActorSystem.Create("Simulator",configuration)
let port = "9090"
let addr = "akka.tcp://ServerEngine@" + ip.ToString() + ":" + port + "/user/requestHandler"
let serverRef = system.ActorSelection(addr)
let hashTagsList = ["covid19";"crypto";"distributedsystems";"gogators";"blackfriday";"mondaymotivation";"traveltuesday";]
let messages = ["Lockdown has been lifted";"Cyrpto is unpredictable";"The course is great";"Gators wont back down";"Great Deals are out";"Got go to office";"Lets visit a new place"]
let key = [|58uy; 200uy; 140uy; 209uy; 113uy|]
let xorer = (fun i c -> c ^^^ key.[i%key.Length])

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
    |RegisterUser
    |SetUserName of String
    |Login
    |Logout
    |DoTweet of String
    |DoReTweet of String
    |SubscribeTo of String * List<String>
    |HashTagging of String * String
    |MentioningUser of String * String
    |PerformAction
    |SetTime of int
    |Action of int

type UserAdminMessages =
    | InitUsers
    | UserRegistration
    | StartSimulation
    | SetSubscribers
    | PrintAllUsers
    | CheckCount 


let UserActors (mailbox: Actor<_>) =
    let mutable userName = ""
    let mutable bossRef = null
    let mutable timeInterval = 1000 |> float
    let rand = new Random()
    let rec loop() = actor {
        let! message = mailbox.Receive()
        if isNull(bossRef) then
            bossRef <- mailbox.Context.Sender
        match message with 
        | SetUserName name -> userName <- name
        | RegisterUser -> serverRef <! Register (userName,mailbox.Self)
        | Login  -> serverRef <! GoOnline userName
        | Logout -> serverRef <! GoOffline userName
        | DoTweet msg -> serverRef <! Tweet (userName,msg)
        | DoReTweet msg -> serverRef <! ReTweet (userName,msg)
        | SubscribeTo (user,subList) -> serverRef <! Subscribe(user,subList)
        | Acknowledgement msg-> bossRef <! CheckCount 
        | SubscribedTweet (user,msg) -> let s = msg |> Convert.FromBase64String |> Array.mapi xorer |> Encoding.Default.GetString 
                                        printfn "Hey %s, here is a tweet from %s : %s" userName user s
        | SubscribedReTweet (user,msg) -> let s = msg |> Convert.FromBase64String |> Array.mapi xorer |> Encoding.Default.GetString 
                                          printfn "Hey %s, %s Retweeted this : %s" userName user s
        | HashTagging (msg,tag)-> serverRef <! HashTag (userName,msg,tag)
        | HashTagTweet (user,msg,tag) -> let s = msg |> Convert.FromBase64String |> Array.mapi xorer |> Encoding.Default.GetString 
                                         printfn "Hey %s, here is a tweet from %s : %s #%s" userName user s tag
        | MentioningUser (msg,user) -> serverRef <! Mention (msg,user,userName)
        | Mentioned (msg,user) -> let s = msg |> Convert.FromBase64String |> Array.mapi xorer |> Encoding.Default.GetString 
                                  printfn "Hey %s, you have been mentioned by %s in this tweet: %s" userName user s
        | QueryMentionResults (list) ->  let mutable res = "Result: "
                                         for i in list do 
                                            let s = i |> Convert.FromBase64String |> Array.mapi xorer |> Encoding.Default.GetString 
                                            res <- res + s + " , "
                                         printfn "Mentions so far : %s" res
        | QueryHashTagResults (list) ->  let mutable res = "Result: "
                                         for i in list do
                                            let s = i |> Convert.FromBase64String |> Array.mapi xorer |> Encoding.Default.GetString 
                                            res <- res + s + " , "
                                         printfn "Hashtag query result %s" res
        | QueryTweetsResults (list) ->   let mutable res = "Result: "
                                         for i in list do 
                                            let s = i |> Convert.FromBase64String |> Array.mapi xorer |> Encoding.Default.GetString 
                                            res <- res + s + " , "
                                         printfn "All Tweets so far %s" res
        | SetTime time ->  timeInterval <- float(time)
                           mailbox.Self <! PerformAction                                   
        | PerformAction -> mailbox.Self <! Action (rand.Next(1,9))
        | Action msg -> let randIndex = rand.Next(0,messages.Length)
                        let mutable encryptedMsg = messages.[randIndex]
                        encryptedMsg <- encryptedMsg |> Encoding.Default.GetBytes |> Array.mapi xorer |> Convert.ToBase64String
                        match msg with
                        | 1 -> //printfn "tweet"
                               serverRef <! Tweet (userName,encryptedMsg)
                        | 2 -> //printfn "retweet"
                               serverRef <! ReTweet (userName,encryptedMsg)
                        | 3 -> //printfn "Hashtag"
                               serverRef <! HashTag (userName,encryptedMsg,hashTagsList.[randIndex])
                        | 4 -> //printfn "Mention"
                               let mutable index = rand.Next(1,noOfUsers)
                               let mutable muser = userInitial + index.ToString()
                               while userName = muser do
                                    index <- rand.Next(1,noOfUsers)
                                    muser <- userInitial + index.ToString()
                               serverRef <! Mention (encryptedMsg,muser,userName)
                        | 5 -> //printfn "Query Mentions"
                               serverRef <! QueryMention userName
                        | 6 -> //printfn "Query hashtags"
                               serverRef <! QueryHashTag (userName,hashTagsList.[randIndex])
                        | 7 -> //printfn "Query subscribed tweets"
                               serverRef <! QueryTweets userName
                        | 8 -> //printfn "GoOnline"
                               serverRef <! GoOnline userName
                        | 9 -> //printfn "GoOffline"
                               serverRef <! GoOffline userName
                        | _ -> ()
                        system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(timeInterval), mailbox.Self, PerformAction)
        | _ -> printfn "No type Matched"
        return! loop()
    }
    loop()

let UserAdmin (mailbox: Actor<_>) =
    let mutable userCount = 0
    let mutable iteration = 0
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        |InitUsers ->   for i in [1..noOfUsers] do
                            let userName = userInitial+i.ToString() 
                            actorPool.[i] <- spawn system (userName)  UserActors
                            actorPool.[i] <! SetUserName userName
                            noOfSubscribers.[i] <- (noOfUsers-1)/i
                            //printfn "%d" noOfSubscribers.[i]

                        mailbox.Self <! UserRegistration
        |UserRegistration -> for i in [1..noOfUsers] do 
                               actorPool.[i] <! RegisterUser
                             mailbox.Self <! SetSubscribers
        |SetSubscribers ->  if iteration = 0 then
                                iteration <- iteration + 1
                                userCount <- 0
                                for i in [1..noOfUsers] do
                                    userCount <- userCount + 1
                                    let mutable subScriberCount = 1
                                    let mutable subList = new List<String>()
                                    while subScriberCount <= noOfSubscribers.[i] do
                                        let tempName = userInitial+(i+subScriberCount).ToString()
                                        subList.Add(tempName)
                                        subScriberCount <- subScriberCount + 1
                                    actorPool.[i] <! SubscribeTo (userInitial+i.ToString(),subList)
                                    if userCount = noOfUsers then
                                        //printfn "hello"
                                        //serverRef <! CountSubscribers
                                        actionTime <- Array.rev noOfSubscribers
                                        mailbox.Self <! StartSimulation
        |StartSimulation -> for i in [1..noOfUsers-1] do 
                                //printfn "USer %d Subscribers %d Time %d" i noOfSubscribers.[i] actionTime.[i]
                                actorPool.[i] <! SetTime (actionTime.[i] + noOfUsers)
                            actorPool.[noOfUsers] <! SetTime ((actionTime.[noOfUsers-1] + noOfUsers) + 1)
                            //printfn "User %d Subscribers %d time %d" noOfUsers noOfSubscribers.[noOfUsers] (actionTime.[noOfUsers-1] + 1)
                            //    actorPool.[1] <! DoTweet "Hi"
                            //    actorPool.[1] <! DoReTweet "Hi"
                            //    actorPool.[1] <! HashTagging (messages.[0],hashTagsList.[0])
                            //    actorPool.[1] <! MentioningUser (messages.[0],"User_100")
        |CheckCount -> if(userCount = noOfUsers) then
                            mailbox.Self <! SetSubscribers
                       userCount <- userCount + 1
        |PrintAllUsers -> serverRef <! PrintUsers
        | _ -> printfn "No type matched"
        return! loop()
    }
    loop()

let simulator = spawn system "SimulatorAdmin" UserAdmin

simulator <! InitUsers

system.WhenTerminated.Wait()
