#if INTERACTIVE
#r @"bin\MCD\Debug\netcoreapp3.1\Akka.FSharp.dll"
#r @"bin\MCD\Debug\netcoreapp3.1\Akka.dll"
#r @"bin\MCD\Debug\netcoreapp3.1\FSharp.Core.dll"
#endif

open System
open Akka
open Akka.FSharp
open Akka.Actor


let mutable algorithm = fsi.CommandLineArgs.[3]
let mutable topology = fsi.CommandLineArgs.[2]
let mutable n_nodes= fsi.CommandLineArgs.[1]|>int

let mutable matrix : int[,] = array2D [ [  ]; [ ] ]
let mutable thread_count = 0

let mutable flag = true
type ActorMsg =
    | Done
    | Start

//enum for Sub Actor message
type ActorMsg2 =
    | Rumor
    | Terminate of int
    | Ratio of float32*float32

let system = ActorSystem.Create("MainActor")
let mutable Actor=[]

let sub_actor system name=
    let my_id = (name|>int)
    let mutable index = 0
    let mutable rumor_count = 0
    let mutable neighbors = []
    let random = System.Random()
    //for push sum
    let mutable s = my_id|>float32 
    let mutable w = 1.0f 
    let mutable previous = 0.0f
    let mutable diff = 0.0f
    let mutable currSumEstimate = 0.0f
    let mutable term_count = 0
    let mutable stop_transmit = false
    if topology.Equals("full", StringComparison.OrdinalIgnoreCase) then
                                        let neighbors1 = [1 .. (my_id-1)]
                                        let neighbors2 = [(my_id+1) .. n_nodes]
                                        neighbors <- neighbors1 @ neighbors2    
    if topology.Equals("line", StringComparison.OrdinalIgnoreCase) then
                                        if my_id > 1 && my_id < n_nodes then
                                            neighbors <- [my_id-1 .. my_id+1]
                                        else if my_id = 1 then
                                            neighbors <- [2]
                                        else if my_id = n_nodes then
                                            neighbors <- [n_nodes-1]
    if topology.Equals("2D", StringComparison.OrdinalIgnoreCase) then
                                        
                                        printfn "rth"
                                        //neighbors <- [my2DArray[my_id-1][j], my2DArray[my_id+1][j], my2DArray[i][my_id-1], my2DArray[i][my_id+1]]
    if topology.Equals("imp2D", StringComparison.OrdinalIgnoreCase) then
                                        printfn "dcc"
    spawn system name <|fun mailbox ->
                            let rec loop()=
                                actor{
                                    let! message = mailbox.Receive()
                                    match message with
                                    |Rumor ->      
                                            rumor_count<-rumor_count+1
                                            if stop_transmit then
                                                mailbox.Sender().Tell(Terminate my_id)
                                            else
                                                if rumor_count = 10 then
                                                    stop_transmit <- true
                                                    //printfn "ten:%d" my_id
                                                    mailbox.Sender().Tell(Terminate my_id)
                                                    let M_Actor = system.ActorSelection("akka://MainActor/user/M_Actor")
                                                    M_Actor.Tell(Done)
                                                else if rumor_count<10 then
                                                    //printfn "t:%d" my_id
                                                    index <- random.Next(neighbors.Length)
                                                    let str = "akka://MainActor/user/M_Actor/" + (neighbors.[index]|>string)
                                                    let sel_Actor = system.ActorSelection(str)
                                                    sel_Actor.Tell(Rumor)                                                             
                                    |Ratio (a,b) -> 
                                            if stop_transmit then
                                                mailbox.Sender().Tell(Terminate my_id)
                                            else
                                                s <- s + a
                                                w <- w + b
                                                currSumEstimate <- s/w
                                                diff <- currSumEstimate - previous
                                                let check = (Math.Pow(10|>float,-10|>float))|>float32
                                                if diff < check then
                                                    //terminate
                                                    term_count <- term_count + 1
                                                else
                                                    term_count <- 0
                                                
                                                if term_count < 3 then
                                                    s <- s/(2|>float32)
                                                    w <- w/(2|>float32)
                                                    previous <- currSumEstimate
                                                    index <- random.Next(neighbors.Length)
                                                    let str = "akka://MainActor/user/M_Actor/" + (neighbors.[index]|>string)
                                                    let sel_Actor = system.ActorSelection(str)
                                                    sel_Actor.Tell(Ratio (s, w))  
                                                else
                                                   //terminate
                                                   stop_transmit <- true
                                                   mailbox.Sender().Tell(Terminate my_id)
                                                   let M_Actor = system.ActorSelection("akka://MainActor/user/M_Actor")
                                                   M_Actor.Tell(Done)
                                    |Terminate item ->       
                                            //printfn ":%d" my_id
                                            //printfn "inside terminate"
                                            let newlist = List.filter (fun x -> x<>item) neighbors
                                            neighbors <- newlist
                                            //printfn "nbrl:%d newl:%d" neighbors.Length newlist.Length
                                            //printfn "%A" neighbors
                                            if neighbors.Length>0 then
                                                index <- random.Next(neighbors.Length)
                                                let str = "akka://MainActor/user/M_Actor/" + (neighbors.[index]|>string)
                                                let sel_Actor = system.ActorSelection(str)
                                                sel_Actor.Tell(Rumor)
                                    return! loop()

                                }
                            loop()


let Master_Actor num_of_node= spawn system "M_Actor" <| fun mailbox -> //Main Actor created
        Actor <-
            [1..num_of_node]
            |> List.map(fun id-> sub_actor mailbox ((string(id)))) 
        
        let random = System.Random()
        let mutable index = random.Next(Actor.Length)
        let stopWatch = System.Diagnostics.Stopwatch.StartNew()
        printfn "%f" stopWatch.Elapsed.TotalMilliseconds  
        let rec loop()=
            actor{
                let! message = mailbox.Receive()
                match message with
                |Start-> //schedule initial set of sub problems to sub actors
                    if algorithm.Equals("gossip", StringComparison.OrdinalIgnoreCase) then
                        Actor.[index].Tell(Rumor)
                    else if algorithm.Equals("push-sum", StringComparison.OrdinalIgnoreCase) then                              
                        Actor.[index].Tell(Ratio (0|>float32,0|>float32))
                |Done -> 
                     thread_count <- thread_count + 1 
                     printfn "t_count:%d" thread_count
                     if thread_count = n_nodes-1 then
                        printfn "END REACHED:%f" stopWatch.Elapsed.TotalMilliseconds                        
                        flag <- false
                return! loop()
            }
        loop()
 
let isPerfect (N:float) = 
        if (sqrt(N)-floor(sqrt(N))) = 0.0 then
            true
        else
            false


let mutable matrix_dim = n_nodes
//for 2D finding the pefect square > or = no. of nodes
// if topology.Equals("2D") || topology.Equals("imp2D") then
//     if isPerfect(matrix_dim|>float)=false then
//     		matrix_dim <- matrix_dim+1
//     		let mutable continueLooping = true
//     		while continueLooping do
//     			if isPerfect(matrix_dim|>float) then
//     				continueLooping <- false
//     			else
//     				matrix_dim <- matrix_dim+1

// 	matrix_dim <- (Math.Sqrt(matrix_dim|>float))|>int
// 	let abc = Array2D.zeroCreate<int> matrix_dim matrix_dim
// 	let mutable count = 1
// 	for i = 0 to matrix_dim-1 do
// 		for j = 0 to matrix_dim-1 do
// 			if count <= n_nodes then
// 				abc.[i,j] <- count
// 				count <- count + 1
// 	matrix <- abc
  

Master_Actor n_nodes |>ignore
let M_Actor = system.ActorSelection("akka://MainActor/user/M_Actor")
M_Actor.Tell(Start);
while(flag) do 
    printf ""