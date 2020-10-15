// Learn more about F# at http://fsharp.org

open System
open Akka
open Akka.FSharp
open Akka.Actor

let mutable matrix : int[,] = array2D [ [  ]; [ ] ]
let mutable thread_count = 0
let mutable algorithm = ""
let mutable topology = ""
let mutable n_nodes= 0
let start_rumor = "hey"
let mutable flag = 0
type ActorMsg =
    | Done
    | Start

//enum for Sub Actor message
type ActorMsg2 =
    | Rumor of string
    | Ratio of float32*float32

let system = ActorSystem.Create("MainActor")
let mutable Actor=[]

let sub_actor system name=
    let my_id = (name|>int)
    let mutable index=0
    let mutable rumor_count = 0
    let mutable neighbors=[]
    let random = System.Random()
    //for push sum
    let mutable s = my_id|>float32 
    let mutable w = 1.0f 
    let mutable previous = 0.0f
    let mutable diff = 0.0f
    let mutable currSumEstimate = 0.0f
    let mutable term_count = 0
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
                                        printf "dcc"
    spawn system name <|fun mailbox ->
                            let rec loop()=
                                actor{
                                    let! message = mailbox.Receive()
                                    match message with
                                    |Rumor r_msg->      
                                           if algorithm.Equals("gossip", StringComparison.OrdinalIgnoreCase) then
                                                if r_msg = start_rumor then
                                                        rumor_count<-rumor_count+1
                                                 
                                                if rumor_count = 10 then
                                                    let M_Actor = system.ActorSelection("akka://MainActor/user/M_Actor")
                                                    M_Actor.Tell(Done)
                                                if rumor_count<10 then
                                                    index <- random.Next(neighbors.Length)
                                                    let str = "akka://MainActor/user/M_Actor/" + (neighbors.[index]|>string)
                                                    let sel_Actor = system.ActorSelection(str)
                                                    sel_Actor.Tell(Rumor r_msg)
                                                    
                                           if algorithm.Equals("pushsum", StringComparison.OrdinalIgnoreCase) then     
                                               if r_msg = start_rumor then
                                                rumor_count<-rumor_count+1
                                                index <- random.Next(neighbors.Length)
                                                let str = "akka://MainActor/user/M_Actor/" + (neighbors.[index]|>string)
                                                let sel_Actor = system.ActorSelection(str)
                                                currSumEstimate <- s/w
                                                previous <- currSumEstimate
                                                let x = 2.0f
                                                sel_Actor.Tell(Ratio (s/x, w/x))                                                             
                                    |Ratio (a,b) -> 
                                            s <- s + a
                                            w <- w + b
                                            currSumEstimate <- s/w
                                            rumor_count<-rumor_count+1
                                            diff <- currSumEstimate - previous
                                            let check = (Math.Pow(10|>float,-10|>float))|>float32
                                            if diff < check then
                                                //terminate
                                                term_count <- term_count + 1
                                            else
                                                term_count <- 0
                                            
                                            if term_count < 3 then
                                                previous <- currSumEstimate
                                                
                                                index <- random.Next(neighbors.Length)
                                                let str = "akka://MainActor/user/M_Actor/" + (neighbors.[index]|>string)
                                                let sel_Actor = system.ActorSelection(str)
                                                let x = 2.0f
                                                sel_Actor.Tell(Ratio (s/x, w/x))  
                                            else
                                               //terminate
                                               let M_Actor = system.ActorSelection("akka://MainActor/user/M_Actor")
                                               M_Actor.Tell(Done)
                                                        
                                    return! loop()

                                }
                            loop()


let Master_Actor num_of_node= spawn system "M_Actor" <| fun mailbox -> //Main Actor created
        Actor <-
            [1..num_of_node]
            |> List.map(fun id-> sub_actor mailbox ((string(id)))) 
        
        let random = System.Random()
        let mutable index = random.Next(Actor.Length)
        let rec loop()=
            actor{
                let! message = mailbox.Receive()
                match message with
                |Start-> //schedule initial set of sub problems to sub actors
                     Actor.[index].Tell(Rumor start_rumor)                              
                     
                |Done -> 
                     thread_count <- thread_count + 1 
                     if thread_count = n_nodes then
                        flag <- 2
                return! loop()
            }
        loop()
 
let isPerfect (N:float) = 
        if (sqrt(N)-floor(sqrt(N))) = 0.0 then
            true
        else
            false

let main argv =
      
    algorithm <- argv[3]
    n_nodes <- argv[1]|>int
    topology <- argv[2]
    let mutable matrix_dim = n_nodes
    //for 2D finding the pefect square > or = no. of nodes
    if topology.Equals("2D") then
        if isPerfect(matrix_dim|>float)=false then
            matrix_dim <- matrix_dim+1
            let mutable continueLooping = true
            while continueLooping do
                if isPerfect(matrix_dim|>float) then
                    continueLooping <- false
                else
                    matrix_dim <- matrix_dim+1
        matrix_dim <- (Math.Sqrt(matrix_dim|>float))|>int
        let abc = Array2D.zeroCreate<int> matrix_dim matrix_dim
        let mutable count = 1
        for i = 0 to n_nodes-1 do
            for j = 0 to n_nodes-1 do
                abc.[i,j] <- count
                
        matrix <- abc
      

    Master_Actor n_nodes |>ignore
    let M_Actor = system.ActorSelection("akka://MainActor/user/M_Actor")
    M_Actor.Tell(Start)

    while(flag=0) do

   
    0 // return an integer exit code
