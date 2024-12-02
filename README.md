# XV6
## LAZY Fork
You’ve just been recruited as an unpaid intern by LAZY Corp, the most relaxed and laid-back company in the world of operating systems. Here at LAZY Corp, we believe in doing things the easy way, and we’re always looking for clever ways to save time and resources. Our latest project is all about maximizing efficiency with minimal effort, and that’s where you come in.

Your mission, should you choose to accept it, is to implement a Copy-On-Write (COW) fork for our version of xv6. Here’s the deal: our current fork system call creates a perfect clone of the parent process, including a full copy of all its memory. While this works, it’s far from lazy—it consumes way too much memory and isn’t exactly the most efficient approach.

At LAZY Corp, we’ve come up with a better solution: Copy-On-Write. Here’s how it works:

When the parent process forks, instead of making a full copy of all the memory pages, we’ll have both processes share the same pages initially.
These shared pages will be marked as read-only and flagged as “copy-on-write.” This means the processes can share memory until one of them tries to make changes.
If a process tries to write to one of these shared pages, the RISC-V CPU will raise a page-fault exception.
At that point, the kernel will jump in to make a duplicate of the page just for that process and map it as read/write, allowing the process to modify its own copy without affecting the other.
By using COW, you’ll help LAZY Corp save memory and let our system run smoother. Plus, you’ll be making things as efficient and lazy as possible—because here at LAZY Corp, we believe in doing more with less.

# Concurrency
## 1. LAZY Read-Write
Great job on the COW fork! Now that you’ve implemented the Copy-On-Write fork, LAZY Corp has another exciting challenge for you. While the COW fork helped reduce memory overhead, we’re still dealing with performance issues in other parts of our operating system—especially the file manager.

Our file manager, LAZY (because we are too lazy to name it anything else), runs on an old system with very limited resources. Speed and concurrency are desirable, but this old system struggles to handle too many users at once. However, LAZY doesn’t care—our goal is to stay efficient and relaxed, even if that means a few users might have to cancel their requests out of sheer frustration!

Your next mission is to simulate LAZY’s behavior under load. You’ll simulate how our file manager processes requests like reading, writing, and deleting files with multiple users trying to access them simultaneously. Since we don’t want to overwork LAZY, only a limited number of users can access any given file at the same time.

Here’s how LAZY works:

Each operation (READ, WRITE, DELETE) takes some time to process.
There’s a limit on how many users can access the same file concurrently (for example, if 1 person is writing to the file while 2 others are reading, that is to be counted as 3 concurrent accesses).
Users may get impatient and cancel their requests if LAZY doesn’t get around to them in time.
Your task is to simulate this scenario, handle multiple user requests, and manage the concurrency and patience limits. Don’t worry, though—you’ll be using the same laid-back style you learned while implementing the COW fork.

## Input Format
You’ll be provided the following information to simulate LAZY:
``c
r w d
n c T
u_1 f_1 o_1 t_1
u_2 f_2 o_2 t_2
…
u_m f_m o_m t_m
STOP
``
This represents:

`r`, `w`, `d`: The time taken for READ, WRITE, and DELETE operations (in seconds).
`n`: The number of files (named file 1, file 2, …, file n).
`c`: The maximum number of users that can access a single file concurrently.
`T`: The maximum time a user will wait for their request before they cancel it.
`u_i`, `f_i`, `o_i`, `t_i`: Information about each user’s request:
`u_i`: The ID of the user.
`f_i`: The file number they want to access.
`o_i`: The operation (READ, WRITE, DELETE) they want to perform.
`t_i`: The time (in seconds) when they make the request.
The input ends with the word `STOP`.
### Simulation Rules
LAZY’s behavior is defined as follows: 

#### Processing requests:
LAZY waits for 1 second after a request arrives before starting to process it.
If LAZY can’t process the request right away (due to system limitations), it will be delayed.
Users cancel their requests if LAZY takes more than T seconds (from the time at which users send their request) to start processing.<br>
#### Concurrency rules:
Multiple users can READ a file simultaneously, even while another user is writing to it.
Only one user can WRITE to a file at a time.
DELETE operations can only occur if no users are currently reading or writing to the file. Once deleted, the file is gone permanently.<br>
#### Request handling:
READ: The user reads the file if conditions allow.
WRITE: The user writes to the file if no other write operation is in progress.
DELETE: The user deletes the file if it’s not being read or written to. While the file is being deleted, no other user can read/write to the same file.
If the file is invalid (i.e., deleted or doesn’t exist), LAZY declines the request when it attempts to process it.
#### Cancellation:
Users cancel their requests if LAZY doesn’t start processing them within T seconds.
Once LAZY begins processing a request, it can’t be cancelled.

## Output Format
The simulation should output the events that happen in the system in chronological order. These include:

When LAZY File Manager starts processing:
LAZY has woken up!
For each user request:
When a user makes a request:User u_i has made request for performing o_i on file f_i at t seconds [YELLOW]
When LAZY starts processing a request:LAZY has taken up the request of User u_i at t seconds [PINK]
If LAZY declines a request due to an invalid file:LAZY has declined the request of User u_i at t seconds because an invalid/deleted file was requested. [WHITE]
When a request completes:The request for User u_i was completed at t_j seconds [GREEN]
If the user cancels their request:User u_i canceled the request due to no response at t_k seconds [RED]
When all pending requests are finished:

LAZY has no more pending requests and is going back to sleep!

#### Input:


``2 4 6``<br>
``3 2 5``<br>
``1 1 READ 0``<br>
``2 2 WRITE 1``<br>
`3 2 DELETE 2`<br>
`4 1 WRITE 3`<br>
`5 2 READ 4`<br>
`STOP`<br>

#### Output:

`LAZY has woken up!`

<span style="color:yellow">User 1 has made request for performing READ on file 1 at 0 seconds </span><br>
<span style="color:pink">LAZY has taken up the request of User 1 at 1 seconds </span><br>
<span style="color:yellow">User 2 has made request for performing WRITE on file 2 at 1 seconds </span><br>
<span style="color:pink">LAZY has taken up the request of User 2 at 2 seconds</span><br>
<span style="color:yellow">User 3 has made request for performing DELETE on file 2 at 2 seconds</span><br>
<span style="color:green">The request for User 1 was completed at 3 seconds</span><br>
<span style="color:yellow">User 4 has made request for performing WRITE on file 1 at 3 seconds</span><br>
<span style="color:pink">LAZY has taken up the request of User 4 at 4 seconds </span><br>
<span style="color:yellow">User 5 has made request for performing READ on file 2 at 4 seconds</span><br>
<span style="color:pink">LAZY has taken up the request of User 5 at 5 seconds.</span><br>
<span style="color:green">The request for User 2 was completed at 6 seconds.</span><br>
<span style="color:green">The request for User 5 was completed at 7 seconds. </span><br>
<span style="color:red">User 3 canceled the request due to no response at 7 seconds.</span><br>
<span style="color:green">The request for User 4 was completed at 8 seconds.</span>

`LAZY has no more pending requests and is going back to sleep!`

## 2. LAZY Sort
After your success with the LAZY file manager simulation, the higher-ups at LAZY Corp have a new challenge for you. Our systems are accumulating files at a rapid pace, and it’s time to implement a sorting mechanism that aligns with our core philosophy: do things the lazy, efficient way.

Here’s the deal: we need a distributed sorting algorithm to organize all the files in the system. But remember, here at LAZY Corp, we only put in the extra effort when it’s really necessary. If there are only a few files, we want to keep things simple and avoid unnecessary complexity. But when things get hectic, we’ll bring in a more powerful sorting strategy.

#### Your Task
##### Lazy Sorting Strategy: 
The system should dynamically decide which sorting algorithm to use based on the number of files:
If the number of files is below a certain threshold, use Distributed Count Sort. This method is straightforward, minimal effort, and works just fine for smaller file counts. We don’t need to break a sweat on fancy sorting unless we really have to.
If the number of files exceeds the threshold, implement Distributed Merge Sort. This approach is more robust and can handle larger datasets effectively. When the workload increases, we rely on this more capable sorting method to get the job done.
##### Distributed Implementation: 
Since we’re dealing with a distributed system, your solution should be capable of coordinating multiple nodes to perform the sorting. The files are scattered across different machines, so you’ll need to make sure that the sorting algorithm distributes tasks efficiently and gathers the sorted results at the end.
##### Lazy Allocation: 
To stay true to our ethos, the sorting should proceed in a way that avoids unnecessary resource usage. Each node should only be activated when needed, and sorting tasks should be delegated in a way that minimizes overall workload.
### Input Format
The input begins with an integer specifying the total number of files. Each file is then represented on a separate line with its attributes separated by spaces in the following order: name, id, and timestamp. The final row contains the column to be used for sorting One of [”Name”, “ID”, “Timestamp”]<br>
The timestamp should be in ISO 8601 format (`YYYY-MM-DDTHH:MM:SS`).
The file name can be a string of max length 128 char<br>
`5`<br>
`fileA.txt 101 2023-10-01T14:30:00`<br>
`fileB.txt 102 2023-10-01T12:15:00`<br>
`fileC.txt 103 2023-09-29T09:45:00`<br>
`fileD.txt 104 2023-10-02T17:05:00`<br>
`fileE.txt 105 2023-09-30T10:20:00`<br>
`ID`<br>
## Output Format
The output consists of:<br>
The name of the sorting column used, indicated on the first line.
The sorted list of files, each on a new line with name, id, and timestamp in the same order as the input.
Each attribute is separated by a space, with a newline separating each file entry<br>
`ID` <br>
`fileA.txt 101 2023-10-01T14:30:00`<br>
`fileB.txt 102 2023-10-01T12:15:00`<br>
`fileC.txt 103 2023-09-29T09:45:00`<br>
`fileD.txt 104 2023-10-02T17:05:00`<br>
`fileE.txt 105 2023-09-30T10:20:00`<br>
## Sorting Criteria
Distributed Count Sort should be used when the total number of files is below a threshold of 42.<br>
Distributed Merge Sort should be used when the total number of files exceeds the threshold of 42.

