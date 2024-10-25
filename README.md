Iterative implementation of a folder replication algorithm, which
uses a stack to hold tuples of directory paths and a bool to check if the to-be copied directory is newly created in the replica folder in order to avoid the file/folder deletion steps. 
The algorithm generates a HashSet if the number of files/directories is greater than the set limit in order to improve the performance of the file comparison step. 
It also parallelizes the file deletion and copying steps for better performance.
