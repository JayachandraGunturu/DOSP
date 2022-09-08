open System.Threading
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Collections.Generic


let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")

type BossActorMessages = 
    | GenerateActorPool
    | AllocateNeighbors
    | GenerateFingerTables
    | Lookup
    | FoundKey of int
    | KEYNOTFOUND of int * int * int * int
    | JoinNewNode
    | DeleteNode
    | Stabilize

type ActorMessages = 
    | SetNeighbors of IActorRef * IActorRef * int * int * int
    | SetFingerTable of int
    | Search of int * int * int

let system = ActorSystem.Create("System",configuration)
let mutable nodes = int (string (fsi.CommandLineArgs.GetValue 1))
let requests = int (string (fsi.CommandLineArgs.GetValue 2))

let m = int(Math.Log2(float(nodes)))
let mutable actorPool = [||]
let mutable sortedNodeActors = [||]
let mutable sortedHashesNodes: int[] = [||]
sortedHashesNodes <- Array.zeroCreate (nodes + 1)
sortedNodeActors <- Array.zeroCreate (nodes + 1)
let ActorHashMap = new SortedDictionary<int64, IActorRef>()
actorPool <- Array.zeroCreate (nodes + 1)

let getKey(hexArray:string[]) =
    let mutable res = ""
    for hex in hexArray do
        res <- res+(Convert.ToInt32(hex,16).ToString())
    res.ToString().Substring(res.Length - m)

let getRandomIPV4(id: int) = 
    let part1 = Random().Next(256).ToString()
    let part2 = Random().Next(256).ToString()
    let part3 = Random().Next(256).ToString()
    let part4 = Random().Next(256).ToString()
    let randomIPV4 = part1 + ":" + part2 + ":" + part3 + ":" + part4
    randomIPV4

let getSHA1(actorID:int) =
    let actorIP = getRandomIPV4(actorID)
    let sha = System.Security.Cryptography.SHA1.Create()
    let sourceBytes = System.Text.Encoding.UTF8.GetBytes(actorIP);
    let hashBytes = sha.ComputeHash(sourceBytes);
    let hash = BitConverter.ToString(hashBytes).ToLower();
    let hexArray = hash.Split("-")
    let mutable res = int64(getKey(hexArray))
    res <- res % (int64(2) * int64(nodes))
    res

let getKeys()=
    for i in 0..nodes-1 do
        let mutable res = getSHA1(i)

        while(ActorHashMap.ContainsKey(res)) do
            res <- getSHA1(i)
        ActorHashMap.Add(res,actorPool.[i])

    let mutable index = 0

    for i in ActorHashMap.Keys do
        sortedNodeActors.[index] <- ActorHashMap.[i]
        index <- index + 1

let getNewKey()=
    let mutable res = getSHA1(nodes)
    while(ActorHashMap.ContainsKey(res)) do
        res <- getSHA1(nodes)
    ActorHashMap.Add(res, actorPool.[nodes])
    sortedNodeActors.[nodes] = ActorHashMap.[res]

let Actor(mailbox: Actor<_>) =
    let mutable successor = null
    let mutable predecessor = null
    let mutable currentHash = 0
    let mutable successorHash = 0
    let mutable predecessorHash = 0
    let mutable fingerTable = new SortedDictionary<int, int>()
    let mutable bossRef: IActorRef = null
    let mutable prevReqId = new SortedDictionary<int, int>()
    
    let rec loop()= actor{
        let! msg = mailbox.Receive();
        if isNull bossRef then
            bossRef <- mailbox.Sender()
        match msg with
        |SetNeighbors (p,s,pHash,sHash, cHash) -> predecessor <- p
                                                  successor <- s
                                                  predecessorHash <- pHash
                                                  successorHash <- sHash
                                                  currentHash <- cHash

        |SetFingerTable (id) ->         let ele = id
                                        fingerTable.Clear()
                                        let node = Array.BinarySearch(sortedHashesNodes, ele)
                                        if node < 0 then
                                            fingerTable.Add(ele, sortedHashesNodes.[(-node-1) % nodes])
                                        else
                                            fingerTable.Add(ele, sortedHashesNodes.[node])
                                
                                        for i in 0..m-1 do
                                            let ele = id + int(Math.Pow(float(2), float(i)))
                                            let node = Array.BinarySearch(sortedHashesNodes, ele)
                                            if node < 0 then
                                                fingerTable.Add(ele, sortedHashesNodes.[(-node-1) % nodes])
                                            else
                                                fingerTable.Add(ele, sortedHashesNodes.[node])

        |Search (key, count, reqID) ->  if not (prevReqId.ContainsKey(reqID)) then    
                                            prevReqId.Add(reqID, 1)
                                            let mutable nearestKey = -1
                                            let mutable flag = false
                                            for k in fingerTable.Keys do
                                                if fingerTable.Item(k) = key && not flag then
                                                    flag <- true
                                                    bossRef <! FoundKey (count)
                                                elif fingerTable.Item(k) > key && not flag then
                                                    flag <- true
                                                    if nearestKey <> -1 then
                                                        nearestKey <- nearestKey
                                                    else
                                                        nearestKey <- successorHash
                                                    bossRef <! KEYNOTFOUND (nearestKey, key, count, reqID)
                                                else nearestKey <- fingerTable.Item(k)
                                            if not flag then
                                                bossRef <! KEYNOTFOUND (nearestKey, key, count, reqID)
                                        else
                                            bossRef <! KEYNOTFOUND (key, key, 0, reqID)
        return! loop()
    }
    loop()

let BossActor(mailbox: Actor<_>) =
    let mutable counter = 0
    let mutable count = 0
    let mutable totalCount = float(0)
    let rec loop()= actor{
        let! msg = mailbox.Receive();
        match msg with 
        |GenerateActorPool ->   printfn "intiated actors"
                                for i in [0..nodes-1] do
                                    let actorID = "" + i.ToString()
                                    actorPool.[i] <- spawn system actorID  Actor
                                getKeys() |> ignore
                                let mutable i = 0
                                for key in ActorHashMap.Keys do
                                    sortedHashesNodes.[i] <- int(key)
                                    i <- i + 1

        |AllocateNeighbors ->   sortedNodeActors.[0] <! SetNeighbors(sortedNodeActors.[nodes-1],sortedNodeActors.[1], sortedHashesNodes.[nodes-1], sortedHashesNodes.[1], sortedHashesNodes.[0])
                                sortedNodeActors.[nodes-1] <! SetNeighbors(sortedNodeActors.[nodes-2],sortedNodeActors.[0], sortedHashesNodes.[nodes-2], sortedHashesNodes.[0], sortedHashesNodes.[nodes-1])
                                for i in 1..nodes-2 do
                                    sortedNodeActors.[i] <! SetNeighbors(sortedNodeActors.[i-1],sortedNodeActors.[i+1], sortedHashesNodes.[i-1], sortedHashesNodes.[i+1], sortedHashesNodes.[i])
        
        |GenerateFingerTables ->    for key in sortedHashesNodes.[0..nodes-1] do
                                        ActorHashMap.Item(int64(key)) <! SetFingerTable(key)

        |Lookup ->  printfn "Starting look up operations."
                    for i in 0..(nodes - 1) do
                        for j in 0..requests-1 do
                            counter <- counter + 1
                            count <- count + 1
                            let keyIndex = nodes-1-i
                            let key = sortedHashesNodes.[keyIndex]
                            sortedNodeActors.[i] <! Search (key, 1, count)

        |FoundKey (lookedCount) ->  totalCount <- totalCount + float(lookedCount) / float(count)
                                    counter <- counter - 1
                                    //printfn "%d" counter
                                    if counter = 0 then
                                        printfn "No.of hops: %f" totalCount
                                        system.Terminate() |> ignore

        |KEYNOTFOUND (nearestNode, key, lookedCount, reqID) -> ActorHashMap.Item(int64(nearestNode)) <! Search(key, lookedCount + 1, reqID)

        |JoinNewNode -> printfn "Joining a new node into the network."
                        Thread.Sleep(2000)
                        let actorID = "" + nodes.ToString()
                        actorPool.[nodes] <- spawn system actorID  Actor
                        let mutable res = getSHA1(nodes)
                        while(ActorHashMap.ContainsKey(res)) do
                            res <- getSHA1(nodes)
                        ActorHashMap.Add(res, actorPool.[nodes])
                        sortedNodeActors.[nodes] <- ActorHashMap.[res]
                        sortedHashesNodes.[nodes] <- int(res)
                        printfn "New node created and added to the network."
                        mailbox.Context.Self <! Stabilize

        |DeleteNode ->  printfn "Deleting a node from the network."
                        Thread.Sleep(2000)
                        ActorHashMap.Remove(int64(sortedHashesNodes.[nodes])) |> ignore
                        sortedNodeActors.[nodes] <- null
                        sortedHashesNodes.[nodes] <- 0
                        printfn "Deleted a node from the network."
                        mailbox.Context.Self <! Stabilize
                        

        |Stabilize ->   mailbox.Context.Self <! AllocateNeighbors
                        mailbox.Context.Self <! GenerateFingerTables
        return! loop()
    }
    loop()

let boss = spawn system "BossActor" BossActor

boss <! GenerateActorPool
boss <! AllocateNeighbors
boss <! GenerateFingerTables
boss <! JoinNewNode
boss <! DeleteNode
boss <! Lookup
system.WhenTerminated.Wait()