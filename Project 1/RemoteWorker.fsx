#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.Remote"

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp

let myIP = "192.168.0.118"
let port =  9090 |>string
let mutable ref = null
let mutable serverRef = null
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            stdout-loglevel : OFF
            loglevel : OFF
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                
            }
            remote {
                helios.tcp {
                    port = 9090
                    hostname = 192.168.0.118
                }
            }
        }")

let system = ActorSystem.Create("RemoteWorker", configuration)

let randomStr n =
    let rand = Random()
    let chars = "ABCDEFGHIJKLMNOPQRSTUVWUXYZ0123456789"
    let charsLen = chars.Length
    let randomChars = [|for i in 0..n -> chars.[rand.Next(charsLen)]|]
    let randomString= String(randomChars)
    randomString

let sha256: string -> string =
    let byteToHex: byte -> string = fun b -> b.ToString("x2")

    let bytesToHex: byte array -> string =
        fun bytes ->
            bytes
            |> Array.fold (fun a x -> a + (byteToHex x)) ""

    let utf8ToBytes: string -> byte array = System.Text.Encoding.UTF8.GetBytes

    let tosha256: byte array -> byte array =
        fun bytes ->
            use sha256 =
                System.Security.Cryptography.SHA256.Create()

            sha256.ComputeHash(buffer = bytes)

    fun utf8 -> utf8 |> (utf8ToBytes >> tosha256 >> bytesToHex)

type ActorMsg =
    | Done of string
    | StartWorkers of int

type WorkerMsg =
    |WorkDone of string
    |NoRemoteWorker of string

type ClientMsg=
    |GetK of string
    |Start of int


let checkZeroes (mailbox:Actor<_>)=
    let rec loop()=actor{
        let! msg = mailbox.Receive()
        let k = int(msg)
        let mutable flag = true
        while(flag) do
            let str = "sirrinki;" + randomStr 8
            let encoded = sha256 str
            for i = 0 to k-1 do
                if (encoded.[i] <> '0') then 
                    flag <- false

            if(flag) then
                flag<-false
                let res = str+","+encoded
                mailbox.Context.Sender <! Done res
            else
                flag <- true
    }
    loop()

let remoteWorkers (mailbox:Actor<_>) =
    let rec loop()=actor{
        let! message = mailbox.Receive()
        match message with
            |Done hashVal ->serverRef <! (hashVal+","+myIP)
                            mailbox.Context.Self.Tell(PoisonPill.Instance, mailbox.Self)
                            mailbox.Context.Stop(mailbox.Context.Self) |> ignore
                            mailbox.Context.Stop(mailbox.Context.Parent) |> ignore
                            system.Terminate() |> ignore
        
            |StartWorkers k ->  let workersList=[for i in 1..1000 do yield(spawn system ("Job" + i.ToString())) checkZeroes]
                                for i in 1..1000 do 
                                    workersList.Item(i|>int) <! k
        
        return! loop()
    }
    loop()

let workersRef = spawn system "localWorkers" remoteWorkers

let workers = 
    spawn system "workers"
    <| fun mailbox ->
        let rec loop() =
            actor {
                let! msg = mailbox.Receive()
                let k = int(msg)
                workersRef <! StartWorkers k
                ref <- mailbox.Sender()
                return! loop() 
            }
        loop()


let getInputFromServer = 
 spawn system "getInputFromServer"
    <| fun mailbox ->
        let rec loop() =
            actor {
                let! msg = mailbox.Receive()
                if(msg.ToString().Length>5) then
                    let addr = "akka.tcp://server@" + msg.ToString() + ":" + port + "/user/requestHandler"
                    serverRef <- system.ActorSelection(addr)
                    serverRef <! ("GetK"+","+myIP)
                else
                    printfn "Generating hash with %s leading zeroes " msg
                    workers <! int(msg)

                return! loop() 
            }
        loop()


let serverip = fsi.CommandLineArgs.[1]
getInputFromServer <! (serverip)

system.WhenTerminated.Wait()