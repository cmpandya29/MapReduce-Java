# Matrix Multiplication on Map-Reduce

In this program, args[0] is the first input matrix M, args[1] is the second input matrix N, args[2] is the directory name to pass the intermediate results from the first Map-Reduce job to the second, and args[3] is the output directory. Also the input file format for reading the input matrices and the output format for the final result must be text formats, while the format for the intermediate results between the Map-Reduce jobs must be binary formats. 

There are two small sparce matrices 4\*3 and 3\*3 in the files M-matrix-small.txt and N-matrix-small.txt for testing in small matrices. Their matrix multiplication must return the 4*3 matrix in solution-small.txt. 

Then there are 2 moderate-sized matrices 200*100 and 100*300 in the files M-matrix-large.txt and M-matrix-large.txt for testing in large matrices inputs. Their matrix multiplication must return the matrix in solution-large.txt.

1. Build commands are mentioned in "multiply.build" file.
2. Run commands are mentioned in "multiply.run" file.
