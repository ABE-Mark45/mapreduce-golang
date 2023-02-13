# mapreduce-golang
A MapReduce distributed application based on MIT 6.824 course

## Features
- A single coordinator manages the map and reduce phases
- Multiple worker processes can enter at any given time
- Processes communicate by RPC through unit sockets
- Local file system is utilized to share intermediate data between thhe processes 
