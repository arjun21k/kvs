#!/bin/python3

def main():
    import os
    import sys

    if len(sys.argv) != 2:
        print("Error: Filename missing")
        exit()

    file_name = str(sys.argv[1])
    #print(file_name)
    file1 = open(file_name, 'r')

    count = 0
    total_lat = 0
    for line in file1:
        #print(line)
        if "avg_latency" in line:
            #print(line)
            temp = line.split(' ')
            #print(temp[1])
            if temp[1]:
                #print(temp[1])
                count += 1
                total_lat += float(temp[1])

    print("Average latency = {:.2f} us".format(total_lat/count))

if __name__ == "__main__":
    main()
