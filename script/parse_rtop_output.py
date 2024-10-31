#!/bin/python3

def main():
    import os
    import sys
    import re

    if len(sys.argv) != 2:
        print("Error: Filename missing")
        exit()

    file_name = str(sys.argv[1])
    file1 = open(file_name, 'r')

    count_rx = 0
    count_tx = 0
    tot_rx_gbs = 0
    tot_tx_gbs = 0
    for line in file1:
        #print(line)
        if "dpdk" in line:
            #print(line)
            temp = re.split(r' +', line)
            #print(len(temp))
            rx_gbs = float(temp[5])
            if rx_gbs:
                #print(rx_gbs)
                count_rx += 1
                tot_rx_gbs += rx_gbs
            tx_gbs = float(temp[6])
            if tx_gbs:
                #print(tx_gbs)
                count_tx += 1
                tot_tx_gbs += tx_gbs


    print("Average rxGb/s = {:.2f}".format(tot_rx_gbs/count_rx))
    print("Average txGb/s = {:.2f}".format(tot_tx_gbs/count_tx))

if __name__ == "__main__":
    main()
