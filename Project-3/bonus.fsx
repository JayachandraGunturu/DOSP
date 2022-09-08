#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Diagnostics
open System.Collections.Generic
open System.Security.Cryptography
open System.Numerics


let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            log-dead-letters = 0
            log-dead-letters-during-shutdown = off
        }")

type BossActorMessages = 
    | InitActorPool
    | AssignHash
    | InitImmediateNeighbors of int
    | DeleteNodes
    | StartRequests
    | CheckCount of int

type ActorMessages = 
    | SetNeighbors of IActorRef * IActorRef
    | UpdateFingerTable
    | InitFingerTable
    | SetHashID of int
    | FindKey of int * int
    | Print of int
    | PrintFingerTable
    | TermintateActor

let system = ActorSystem.Create("System",configuration)

let mutable nodes = int(fsi.CommandLineArgs.[1])
let totalrequests = int(fsi.CommandLineArgs.[2])
let m = int(Math.Ceiling(Math.Log(float(nodes)) / Math.Log(2.0)))
let mutable totalHops = 0
let mutable count = 1
let mutable actorPool = [||]
let mutable sortedNodes = [||]
let mutable keys = [||]
let dictionary = new SortedDictionary<int, IActorRef>()
let indexMap = new SortedDictionary<int, int>()
let totalCount = (nodes) * (totalrequests)



sortedNodes <- Array.zeroCreate (nodes)
actorPool <- Array.zeroCreate (nodes)
keys <- Array.zeroCreate (nodes)


let getKey(hexArray:string[]) =
    let mutable res = ""
    for hex in hexArray do
        res <- res+(Convert.ToInt32(hex,16).ToString())
    let len = ((res.Length) - m)
    res.Substring(len)

let getSHA1(actorID:string) =
    let ip1 = (Random().Next(0,256).ToString())
    let ip2 = (":"+(Random().Next(0,256).ToString()))
    let ip3 = (":"+(Random().Next(0,256).ToString()))
    let ip4 = (":"+(Random().Next(0,256).ToString()))
    let nodeIP = ip1 + ip2 + ip3 + ip4
    let sha = System.Security.Cryptography.SHA1.Create()
    let sourceBytes = System.Text.Encoding.UTF8.GetBytes(nodeIP);
    let hashBytes = sha.ComputeHash(sourceBytes);
    let hash = BitConverter.ToString(hashBytes).ToLower();
    let hexArray = hash.Split("-")
    let mutable res = int64(getKey(hexArray))
    res <- (res)%int64(2*nodes)
    res

let Actor(mailbox: Actor<_>) =
    let mutable successor = null
    let mutable predecessor = null
    let mutable hashID = 0
    let mutable fingerTable = [||]
    let sortedFingerTable = new SortedDictionary<int,int>()
    fingerTable <- Array.zeroCreate (m+1)
    let mutable bossRef = null
    let rec loop()= actor{
        let! msg = mailbox.Receive();
        if isNull(bossRef) then
            bossRef <- mailbox.Sender()
        match msg with
        |SetNeighbors (p,s) -> predecessor <- p
                               successor <- s

        |InitFingerTable -> let mutable i = 0
                            sortedFingerTable.Clear()
                            while i < m do
                                let mutable index = ((indexMap.[hashID] + int(Math.Pow(float(2), float(i)))) % nodes)
                                if sortedNodes.[index] <> hashID then
                                    sortedFingerTable.Add(sortedNodes.[index],sortedNodes.[index])
                                    i <- i + 1
                            let mutable index = 0
                            for i in sortedFingerTable.Keys do 
                                fingerTable.[index] <- i
                                index <- index + 1

        |UpdateFingerTable -> printfn "yes"

        |SetHashID id -> hashID <- id

        |FindKey (key,hops)->   let h = hops + 1
                                if key = hashID then
                                    count <- count + 1
                                    totalHops <- totalHops + h
                                    bossRef <! CheckCount count
                                
                                else
                                    let mutable i = 0
                                    let mutable found = false
                                    while i < m do
                                        if fingerTable.[i] = key then
                                            count <- count + 1
                                            totalHops <- totalHops + h
                                            found <- true
                                            i <- m + 100
                                            bossRef <! CheckCount count
                                        if i < m-1 then
                                            if (fingerTable.[i] < key && fingerTable.[i+1] > key) || (fingerTable.[i + 1] < fingerTable.[i]) then
                                                dictionary.[fingerTable.[i]] <! FindKey (key,h)
                                                found <- true
                                                i <- m + 100
                                        i <- i + 1

                                    if not found then
                                        dictionary.[fingerTable.[m-1]] <! FindKey (key,h)
                                    dictionary.[key] <! FindKey (key,h)

        |Print i -> printfn "Neighbors : %s" ((mailbox.Self.ToString() + " ")+predecessor.ToString() + " " + successor.ToString())
        
        |PrintFingerTable -> let mutable res = "" 
                             for i in sortedFingerTable.Keys do
                                res <- res + " "+ (i.ToString())
                             printfn "%s" ((hashID.ToString()) + (" : " + res))
        
        |TermintateActor -> mailbox.Context.Self.Tell(PoisonPill.Instance)
                            mailbox.Context.System.Terminate() |> ignore

        return! loop()
    }            
    loop()

let BossActor(mailbox: Actor<_>) =
    let rec loop()= actor{
        let! msg = mailbox.Receive();
        match msg with 
        |InitActorPool ->  printfn "Starting Building Chord Network"
                           System.Threading.Thread.Sleep(2000)
                           for i in [0..nodes-1] do
                                let actorID = "" + i.ToString()
                                actorPool.[i] <- spawn system actorID  Actor

                           mailbox.Context.Self <! AssignHash
                           
        |AssignHash ->     for i in 0..nodes-1 do
                                let mutable res = getSHA1(i.ToString())
                           
                                while(dictionary.ContainsKey(int(res))) do
                                    res <- getSHA1(i.ToString())
                           
                                dictionary.Add(int(res),actorPool.[i])
                                actorPool.[i] <! SetHashID (int(res))

                           let mutable index = 0
                           for i in dictionary.Keys do
                                sortedNodes.[index] <- i
                                keys.[index] <- i 
                                indexMap.Add(i,index)
                                index <- index + 1

                           mailbox.Context.Self <! InitImmediateNeighbors nodes
                           mailbox.Context.Self <! DeleteNodes

        |InitImmediateNeighbors nodes ->  dictionary.[sortedNodes.[0]] <! SetNeighbors(dictionary.[sortedNodes.[nodes-1]],dictionary.[sortedNodes.[1]])
                                          dictionary.[sortedNodes.[nodes-1]] <! SetNeighbors(dictionary.[sortedNodes.[nodes-2]],dictionary.[sortedNodes.[0]])

                                          for i in 1..nodes-2 do
                                            dictionary.[sortedNodes.[i]] <! SetNeighbors(dictionary.[sortedNodes.[i-1]],dictionary.[sortedNodes.[i+1]])                                    

                                          printfn "Done Building Network"


        |DeleteNodes ->     dictionary.Remove(sortedNodes.[nodes-1]) |> ignore
                            printfn "Deleted a Node......"
                            nodes <- nodes - 1
                            indexMap.Clear()
                            let mutable index = 0
                            for i in dictionary.Keys do
                                sortedNodes.[index] <- i
                                keys.[index] <- i 
                                indexMap.Add(i,index)
                                index <- index + 1
                            mailbox.Context.Self <! InitImmediateNeighbors (nodes-1)
                            printfn "Updated the node network........"
                            for i in 0..nodes-1 do
                                dictionary.[sortedNodes.[i]] <! InitFingerTable    
                            mailbox.Context.Self <! StartRequests

        |StartRequests ->   printfn "Processing requests......."
                            for i in 0..nodes-1 do
                                let actorRef = dictionary.[sortedNodes.[i]]
                                for j in 0..totalrequests-1 do
                                    let key = sortedNodes.[Random().Next(nodes-1)]
                                    actorRef <! FindKey (key,0)
        
        |CheckCount count-> if count = totalCount  then
                                mailbox.Context.System.Terminate() |> ignore
                                for i in dictionary.Keys do
                                    dictionary.[i] <! TermintateActor
                                printfn "All requests processed......."
                                printfn "Average Hop count is %f" (float(totalHops)/float(totalCount))
                                system.Terminate() |> ignore
                   
        return! loop()
    }            
    loop()



let boss = spawn system "BossActor" BossActor

boss <! InitActorPool

system.WhenTerminated.Wait()