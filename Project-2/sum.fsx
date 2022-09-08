#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Diagnostics
open System.Collections.Generic

type GossipMessageTypes =
    | Initailize of IActorRef [] * int
    | CheckCount of String
    | SpreadRumor 
    | CallWorker
    | InitNeighbors of string
    | Initpool of string
    | StartAlgo of string
    | StartRumor
    | DoPushSum of float * float
    | StartPushSum
    | InitPushSum
    | Print

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            stdout-loglevel : off
            loglevel : off
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")

let mutable nodes = int (string (fsi.CommandLineArgs.GetValue 1))
let topology = string (fsi.CommandLineArgs.GetValue 2)
let algorithm = string (fsi.CommandLineArgs.GetValue 3)

let timer = Diagnostics.Stopwatch()
let system = ActorSystem.Create("System",configuration)
let dictionary = new Dictionary<IActorRef, bool>()
let cubeindex = Math.Cbrt(float(nodes)) |> int
let digits = cubeindex.ToString().Length
let mutable  actorPool = [||]

if topology.Contains("3D") then
    nodes <- cubeindex*cubeindex*cubeindex

actorPool <- Array.zeroCreate (nodes + 1)

let getLineNeighbors (i:int) = 
    let mutable neighbors = [||]
    if i = 0 then
        neighbors <- (Array.append neighbors [|actorPool.[i+1]|])
    elif i = nodes then
        neighbors <- (Array.append neighbors [|actorPool.[i-1]|])
    else 
        neighbors <- (Array.append neighbors [| actorPool.[(i - 1)] ; actorPool.[(i + 1 ) ] |] ) 
    neighbors

let getFullNeighbors (id:int) = 
    let mutable neighbors = [||]
    for j in [0..nodes-1] do 
        if id <> j then
            neighbors <- (Array.append neighbors [|actorPool.[j]|])
    neighbors

let getID (a:int) (b:int) (c:int) =
    let neighborID = (Math.Pow(float(10), float(digits)*float(2)))*float(a) + (Math.Pow(float(10), float(digits))*float(b))+(float(1)*float(c)) |> int
    neighborID

let get3DNeighbors (actorID:int) (availableActors:Map<int,IActorRef>) (isImp3D:Boolean)  =
    let mutable neighbors = [||]
    let mutable temp=actorID
    let mutable x =0;
    let mutable y =0;
    let mutable z =0;
    let mutable a =0;
    let mutable b=0;
    let mutable c=0;
    let mutable neighborID = 0;
    let mutable randomIndex = 0;
    z <- floor (float(temp) % Math.Pow(float(10), float(digits))) |> int
    temp <- temp/int(Math.Pow(float(10), float(digits)))
    y <- floor (float(temp) % Math.Pow(float(10), float(digits))) |> int
    temp <- temp/int(Math.Pow(float(10), float(digits)))
    x <- floor (float(temp) % Math.Pow(float(10), float(digits))) |> int
    if x<>1 then
        a<-x-1
        b<-y
        c<-z
        neighborID <- getID a b c
        neighbors <- (Array.append neighbors [|availableActors.[neighborID]|])

    if x<>cubeindex then
        a<-x+1
        b<-y
        c<-z
        neighborID <- getID a b c
        neighbors <- (Array.append neighbors [|availableActors.[neighborID]|])

    if y<>1 then
        a<-x
        b<-y-1
        c<-z
        neighborID <- getID a b c
        neighbors <- (Array.append neighbors [|availableActors.[neighborID]|])

    if y<>cubeindex then
        a<-x
        b<-y+1
        c<-z
        neighborID <- getID a b c
        neighbors <- (Array.append neighbors [|availableActors.[neighborID]|])

    if z<>1 then
        a<-x
        b<-y
        c<-z-1
        neighborID <- getID a b c
        neighbors <- (Array.append neighbors [|availableActors.[neighborID]|])

    if z<>cubeindex then
        a<-x
        b<-y
        c<-z+1
        neighborID <- getID a b c
        neighbors <- (Array.append neighbors [|availableActors.[neighborID]|])

    if isImp3D then
        randomIndex <- Random().Next(0,nodes)
        neighbors <- (Array.append neighbors [|actorPool.[randomIndex]|])
    
    neighbors


let ChildActor(mailbox: Actor<_>) =
    let mutable rumourCount = 0
    let mutable pushSumCount = 0;
    let mutable neighbors =  [||]
    let mutable bossRef = null
    let mutable localS = 0.0
    let mutable localW = 1.0
    let diff = 10.0 ** -10.0
    let mutable converged = false
    let rec loop()= actor{
        let! message = mailbox.Receive();
        if isNull(bossRef) then
            bossRef <- mailbox.Context.Sender
        match message with 

        | Initailize (neighboursList,sval) -> localS <- sval |> float
                                              neighbors <- neighboursList
                                              
        | SpreadRumor -> if rumourCount < 11 then
                                let randIndex = Random().Next(0, neighbors.Length)
                                if not dictionary.[neighbors.[randIndex]] then
                                    neighbors.[randIndex] <! CallWorker
                                mailbox.Self <! SpreadRumor

        | CallWorker ->     if rumourCount = 0 then 
                                mailbox.Self <! SpreadRumor
                            if rumourCount = 10 then 
                                bossRef <! CheckCount "Rumor"
                                dictionary.[mailbox.Self] <- true
                            rumourCount <- rumourCount + 1

        | InitPushSum ->  let index = Random().Next(0, neighbors.Length)
                          localS <- localS / 2.0
                          localW <- localW / 2.0
                          neighbors.[index] <! DoPushSum(localS, localW)

        | DoPushSum (s, w) ->
            let newsum = localS + s
            let newweight = localW + w
            let index = Random().Next(0,neighbors.Length) |> int

            if converged then
                    neighbors.[index] <! DoPushSum(localS/2.0, localW/2.0)
            
            elif ((localS/localW)-(newsum/newweight)) <= diff then
                    pushSumCount <- pushSumCount + 1
                    if  pushSumCount = 3 then
                        converged <- true
                        bossRef <! CheckCount "count"
            else
                pushSumCount <- 0
                 
            localS <- newsum / 2.0
            localW <- newweight / 2.0
            neighbors.[index] <! DoPushSum(localS, localW)
        
        |Print ->    let mutable id = mailbox.Self.ToString().Split("_").[1]
                     for item in neighbors do
                        id <- id + ":"+ item.ToString()
                        printfn "%s" id
        |_ -> ()

        return! loop()
    }            
    loop()


let RandomWorker (mailbox: Actor<_>) =
    let neighbors = new List<IActorRef>()
    let mutable count = 0
    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        | InitNeighbors _ -> for i in [0..nodes-1] do
                                neighbors.Add actorPool.[i]
                             count <- neighbors.Count
                             mailbox.Self <! SpreadRumor

        | SpreadRumor ->    if count >= 0 then
                                let randIndex = Random().Next(neighbors.Count)
                                let actorRef = neighbors.[randIndex]
                                if (dictionary.[neighbors.[randIndex]]) then  
                                    neighbors.Remove actorRef |>ignore
                                    count <- count-1
                                else 
                                    actorRef <! CallWorker
                                mailbox.Self <! SpreadRumor 
        | _ -> ()

        return! loop()
    }
    loop()


let randomWorkerRef = spawn system "RandomGossipWorker" RandomWorker


let BossActor(mailbox: Actor<_>) =
    let mutable count = 0
    let mutable availableActors = Map.empty<int,IActorRef>
    let mutable tempActorList = []
    let mutable actorIDList = []
    let mutable flag = false
    let rec loop()= actor{
        let! msg = mailbox.Receive();
        match msg with 
        |Initpool topology->      if topology = "line" || topology = "full" then
                                    for i in [0..nodes] do
                                        actorPool.[i] <- spawn system ("Actor_" + i.ToString())  ChildActor
                                        dictionary.Add(actorPool.[i], false)
                                  else
                                    let mutable index = 0;
                                    if topology = "imp3D" then
                                        flag <- true
                                    for i in 1..cubeindex  do
                                        for j in 1..cubeindex do
                                            for k in 1..cubeindex do
                                                let actorID= getID i j k
                                                let temp = spawn system ("Actor_" + actorID.ToString()) ChildActor
                                                actorPool.[index] <- temp
                                                index<-index+1
                                                dictionary.Add(temp, false)
                                                tempActorList <- (actorID,temp) :: tempActorList
                                                actorIDList <- actorID :: actorIDList
                                    nodes <- tempActorList.Length            
                                    availableActors <- tempActorList |> Map.ofList
                                  
        |InitNeighbors topology-> if topology = "line" then
                                    for i in [0 .. nodes] do
                                        let mutable neighbors= getLineNeighbors i
                                        actorPool.[i] <! Initailize (neighbors,i)
   
                                  elif topology = "full" then
                                    for i in [0 .. nodes] do
                                        let mutable neighbors = getFullNeighbors i
                                        actorPool.[i]<! Initailize (neighbors,i)

                                  elif topology = "3D" || topology = "imp3D" then
                                    for i in [0.. nodes-1] do
                                        let mutable neighbors = get3DNeighbors actorIDList.[nodes-i-1] availableActors flag
                                        actorPool.[i] <! Initailize (neighbors,i)
                                  
                                  mailbox.Context.Self <! StartAlgo algorithm

        |StartAlgo algo-> if algo = "gossip" then
                            printfn "Starting Gossip"
                            mailbox.Context.Self <! StartRumor
                          else
                            printfn "Starting PushSum"
                            mailbox.Context.Self <! StartPushSum

        |StartRumor->    let randomActor = Random().Next(0, nodes)
                         timer.Start()
                         actorPool.[randomActor] <! SpreadRumor
                         randomWorkerRef<! InitNeighbors "Add neighbors"

        |StartPushSum ->  let randIndex = Random().Next(1, nodes)
                          timer.Start()
                          actorPool.[randIndex] <! InitPushSum

        |CheckCount msg -> count <- count + 1
                           //printfn "%d" count
                           if count = nodes then
                              timer.Stop()
                              printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
                              Environment.Exit(0)
        | _ -> ()
        return! loop()
    }            
    loop()

let boss = spawn system "BossActor" BossActor

boss <! Initpool topology
boss <! InitNeighbors topology

Console.ReadLine() |> ignore
