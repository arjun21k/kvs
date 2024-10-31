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
        #if "avg_latency" in line:
        if "avg_lat" in line:
            #print(line)
            temp = line.split(' ')
            #print(temp[0])
            if temp[0]:
                t = temp[0].split('=')
                #print(t[1])
                count += 1
                total_lat += float(t[1])

    print("Average latency = {:.2f} us".format(total_lat/count))

if __name__ == "__main__":
    main()
