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
    total_mops = 0
    for line in file1:
        #print(line)
        if "tput=" in line:
            #print(line)
            temp = line.split()
            #print(temp[1])
            iops = float(temp[1])
            if iops:
                #print(iops)
                count += 1
                total_mops += iops

    print("Average throughput = {:.2f} Mops".format(total_mops/count))

if __name__ == "__main__":
    main()

