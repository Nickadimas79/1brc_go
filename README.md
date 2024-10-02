# 1brc_go
Iteration on the One Billion Row Challenge but done using Golang.

There are currently four "versions" I have used in testing differnt processing methods. To switch between them you just go to the `main.go` file and comment out the three you don't want to run and leave the one you do.

To create/change the data the code runs use the command `python3 python/create_measurements.py <number_of_lines_wanted>`. The line number should be typed like `100_000`, which would be 100,000.
This will overwrite the `measurements.txt` file with the givenn number of lines of corresponding cities and their tempuratures.


After creating/overwriting the run data just use `go run main.go` and the code should then run. If you wish to test the `fourth` iteration that has flags available for usage and testing data. The only required flag is `input` as this is the file you intend to process. ex: `go run main.go -input=measurements.txt`.
