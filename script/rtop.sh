#!/bin/bash

# Copyright (c) 2019, The Ohio State University. All rights reserved.
#
# This file is part of the rtop software package developed by the team 
# members of Prof. Xiaoyi Lu's group at The Ohio State University.
#
# For detailed copyright and licensing information, please refer to
# the license file LICENSE.txt in the top level directory. 

# Printing usage of this script.

function print_usage(){
    echo "Usage: $(basename $0) [options]" >&2
    cat <<EOF >&2
    -d <devname>
       Specify the RDMA device name to monitor.
    -h
       Show this help message.
    -i <interval>
       Select data records at seconds as close as possible to the number specified by the interval parameter.
    -p <protocol>
       Specify the protocol to monitor; It can be one of these supported protocols: ipoib, rdma, dpdk, or all.
EOF
}

if [ "x$1" == "x" ]
then
   echo "$(basename $0): bad command line argument(s)"
   echo "For more information run $(basename $0) -h"
   exit 0
fi

if [ "x$1" == "x-h" ]
then
  print_usage
  exit 0
fi

# Reading the arguments from the command line.
args=`getopt d:i:p:h $*`

# init parameters
devname="null"
interval=1
protocol="all"

check_dev_validity() {
   dev=$1
   /usr/sbin/ifconfig $dev &> /dev/null
   if [ $? != 0 ]
   then
      echo "Device $dev is invalid. Please double check the parameter."
      return 1
   fi  
}

check_interval_validity() {
   intv=$1
   (($intv > 0))
   if [ $? != 0 ]
   then
      echo "Interval value $intv is invalid. Please double check the parameter."
      return 1
   fi  
}

check_protocol_validity() {
   protc=$1
   if [[ "x${protc}" != "xipoib" && "x${protc}" != "xrdma" && "x${protc}" != "xall" && "x${protc}" != "xdpdk" ]]
   then
      echo "Protocol value $protc is invalid. Please double check the parameter."
      return 1
   fi  
}

set -- $args
for i
do
    case "$i" in
        -d) shift;
            devname=$1
            check_dev_validity $devname
            if [ $? != 0 ]; then exit 1; fi
            shift;;

        -i) shift;
            interval=$1
            check_interval_validity $interval
            if [ $? != 0 ]; then exit 1; fi
            shift;;
        
        -p) shift;
            protocol=$1
            check_protocol_validity $protocol
            if [ $? != 0 ]; then exit 1; fi
            shift;;
    esac
done

# init output values
rx_rdma_pkt_per_sec=0
rx_rdma_kb_per_sec=0
tx_rdma_pkt_per_sec=0
tx_rdma_kb_per_sec=0

rx_ipoib_pkt_per_sec=0
rx_ipoib_kb_per_sec=0
tx_ipoib_pkt_per_sec=0
tx_ipoib_kb_per_sec=0

rx_all_pkt_per_sec=0
rx_all_kb_per_sec=0
tx_all_pkt_per_sec=0
tx_all_kb_per_sec=0

rx_dpdk_pkt_per_sec=0
rx_dpdk_gb_per_sec=0
tx_dpdk_pkt_per_sec=0
tx_dpdk_gb_per_sec=0

max_number(){
        d1=$1
        d2=$2
	d3=$3
        #if (( $(awk 'BEGIN {print ("'$d1'" >= "'$d2'")}') )); then
        if [ $d1 -gt $d2 ] && [ $d1 -gt $d3 ];then
        	echo $d1
        elif [ $d2 -gt $d1 ] && [ $d2 -gt $d3 ];then
        	echo $d2
	else
		echo $d3
        fi
}

G_1=$(bc <<< "1024*1024*1024")
#echo "$G_1"

do_sampling_cal() {
	all_counters=`ethtool -S ${devname}`
        rdma_counters=`echo "${all_counters}" | grep "rdma" | grep "unicast"`
	dpdk_counters=`echo "${all_counters}" | grep "vport_unicast"`

	#echo ${rdma_counters}
	#echo ${dpdk_counters}

	# init counters
	rx_vport_rdma_unicast_packets_s=`echo ${rdma_counters} | cut -d":" -f2 | cut -d" " -f2`
	rx_vport_rdma_unicast_bytes_s=`echo ${rdma_counters} | cut -d":" -f3 | cut -d" " -f2`
	tx_vport_rdma_unicast_packets_s=`echo ${rdma_counters} | cut -d":" -f4 | cut -d" " -f2`
	tx_vport_rdma_unicast_bytes_s=`echo ${rdma_counters} | cut -d":" -f5 | cut -d" " -f2`
	#echo "rdma: $rx_vport_rdma_unicast_packets_s $rx_vport_rdma_unicast_bytes_s $tx_vport_rdma_unicast_packets_s $tx_vport_rdma_unicast_bytes_s"
   
        rx_packets_ipoib_s=`echo "${all_counters}" | grep "rx_packets:" | cut -d":" -f2`
        rx_bytes_ipoib_s=`echo "${all_counters}" | grep "rx_bytes:" | cut -d":" -f2`
        tx_packets_ipoib_s=`echo "${all_counters}" | grep "tx_packets:" | cut -d":" -f2`
        tx_bytes_ipoib_s=`echo "${all_counters}" | grep "tx_bytes:" | cut -d":" -f2`
        #echo "ipoib: $rx_packets_ipoib_s $rx_bytes_ipoib_s $tx_packets_ipoib_s $tx_bytes_ipoib_s"

	# rx_vport_unicast_packets: 6404654423 rx_vport_unicast_bytes: 571859447185 tx_vport_unicast_packets: 8438890640 tx_vport_unicast_bytes: 742611633077
	rx_vport_dpdk_unicast_packets_s=`echo ${dpdk_counters} | cut -d":" -f2 | cut -d" " -f2`
	rx_vport_dpdk_unicast_bytes_s=`echo ${dpdk_counters} | cut -d":" -f3 | cut -d" " -f2`
	tx_vport_dpdk_unicast_packets_s=`echo ${dpdk_counters} | cut -d":" -f4 | cut -d" " -f2`
	tx_vport_dpdk_unicast_bytes_s=`echo ${dpdk_counters} | cut -d":" -f5 | cut -d" " -f2`
	#echo "dpdk: $rx_vport_dpdk_unicast_packets_s $rx_vport_dpdk_unicast_bytes_s $tx_vport_dpdk_unicast_packets_s $tx_vport_dpdk_unicast_bytes_s"

	sleep ${interval}

	all_counters=`ethtool -S ${devname}`
        rdma_counters=`echo "${all_counters}" | grep "rdma" | grep "unicast"`
	dpdk_counters=`echo "${all_counters}" | grep "vport_unicast"`

	#echo ${rdma_counters}

	# init counters
	rx_vport_rdma_unicast_packets_e=`echo ${rdma_counters} | cut -d":" -f2 | cut -d" " -f2`
	rx_vport_rdma_unicast_bytes_e=`echo ${rdma_counters} | cut -d":" -f3 | cut -d" " -f2`
	tx_vport_rdma_unicast_packets_e=`echo ${rdma_counters} | cut -d":" -f4 | cut -d" " -f2`
	tx_vport_rdma_unicast_bytes_e=`echo ${rdma_counters} | cut -d":" -f5 | cut -d" " -f2`
	#echo "rdma: $rx_vport_rdma_unicast_packets_e $rx_vport_rdma_unicast_bytes_e $tx_vport_rdma_unicast_packets_e $tx_vport_rdma_unicast_bytes_e"

        rx_packets_ipoib_e=`echo "${all_counters}" | grep "rx_packets:" | cut -d":" -f2`
        rx_bytes_ipoib_e=`echo "${all_counters}" | grep "rx_bytes:" | cut -d":" -f2`
        tx_packets_ipoib_e=`echo "${all_counters}" | grep "tx_packets:" | cut -d":" -f2`
        tx_bytes_ipoib_e=`echo "${all_counters}" | grep "tx_bytes:" | cut -d":" -f2`
        #echo "ipoib: $rx_packets_ipoib_e $rx_bytes_ipoib_e $tx_packets_ipoib_e $tx_bytes_ipoib_e"

        rx_vport_dpdk_unicast_packets_e=`echo ${dpdk_counters} | cut -d":" -f2 | cut -d" " -f2`
        rx_vport_dpdk_unicast_bytes_e=`echo ${dpdk_counters} | cut -d":" -f3 | cut -d" " -f2`
        tx_vport_dpdk_unicast_packets_e=`echo ${dpdk_counters} | cut -d":" -f4 | cut -d" " -f2`
        tx_vport_dpdk_unicast_bytes_e=`echo ${dpdk_counters} | cut -d":" -f5 | cut -d" " -f2`
        #echo "dpdk: $rx_vport_dpdk_unicast_packets_e $rx_vport_dpdk_unicast_bytes_e $tx_vport_dpdk_unicast_packets_e $tx_vport_dpdk_unicast_bytes_e"

	rx_vport_rdma_unicast_packets_d=$((rx_vport_rdma_unicast_packets_e - rx_vport_rdma_unicast_packets_s))
	rx_vport_rdma_unicast_bytes_d=$((rx_vport_rdma_unicast_bytes_e - rx_vport_rdma_unicast_bytes_s))
	tx_vport_rdma_unicast_packets_d=$((tx_vport_rdma_unicast_packets_e - tx_vport_rdma_unicast_packets_s))
	tx_vport_rdma_unicast_bytes_d=$((tx_vport_rdma_unicast_bytes_e - tx_vport_rdma_unicast_bytes_s))
	#echo "rdma: $rx_vport_rdma_unicast_packets_d $rx_vport_rdma_unicast_bytes_d $tx_vport_rdma_unicast_packets_d $tx_vport_rdma_unicast_bytes_d"

        rx_packets_ipoib_d=$((rx_packets_ipoib_e - rx_packets_ipoib_s))
        rx_bytes_ipoib_d=$((rx_bytes_ipoib_e - rx_bytes_ipoib_s))
        tx_packets_ipoib_d=$((tx_packets_ipoib_e - tx_packets_ipoib_s))
        tx_bytes_ipoib_d=$((tx_bytes_ipoib_e - tx_bytes_ipoib_s))
	#echo "ipoib: $rx_packets_ipoib_d $rx_bytes_ipoib_d $tx_packets_ipoib_d $tx_bytes_ipoib_d" 

        rx_vport_dpdk_unicast_packets_d=$((rx_vport_dpdk_unicast_packets_e - rx_vport_dpdk_unicast_packets_s))
        rx_vport_dpdk_unicast_bytes_d=$((rx_vport_dpdk_unicast_bytes_e - rx_vport_dpdk_unicast_bytes_s))
        tx_vport_dpdk_unicast_packets_d=$((tx_vport_dpdk_unicast_packets_e - tx_vport_dpdk_unicast_packets_s))
        tx_vport_dpdk_unicast_bytes_d=$((tx_vport_dpdk_unicast_bytes_e - tx_vport_dpdk_unicast_bytes_s))
        #echo "dpdk: $rx_vport_dpdk_unicast_packets_d $rx_vport_dpdk_unicast_bytes_d $tx_vport_dpdk_unicast_packets_d $tx_vport_dpdk_unicast_bytes_d"

	rx_rdma_pkt_per_sec=$(bc <<< "scale=2; ${rx_vport_rdma_unicast_packets_d}/${interval}")
	rx_rdma_gb_per_sec=$(bc <<< "scale=2; (${rx_vport_rdma_unicast_bytes_d}/${G_1}/${interval})*8")
	tx_rdma_pkt_per_sec=$(bc <<< "scale=2; ${tx_vport_rdma_unicast_packets_d}/${interval}")
	tx_rdma_gb_per_sec=$(bc <<< "scale=2; (${tx_vport_rdma_unicast_bytes_d}/${G_1}/${interval})*8")

	rx_ipoib_pkt_per_sec=$(bc <<< "scale=2; ${rx_packets_ipoib_d}/${interval}")
	rx_ipoib_gb_per_sec=$(bc <<< "scale=2; (${rx_bytes_ipoib_d}/${G_1}/${interval})*8")
	tx_ipoib_pkt_per_sec=$(bc <<< "scale=2; ${tx_packets_ipoib_d}/${interval}")
	tx_ipoib_gb_per_sec=$(bc <<< "scale=2; (${tx_bytes_ipoib_d}/${G_1}/${interval})*8")

	rx_dpdk_pkt_per_sec=$(bc <<< "scale=2; ${rx_vport_dpdk_unicast_packets_d}/${interval}")
	rx_dpdk_gb_per_sec=$(bc <<< "scale=2; (${rx_vport_dpdk_unicast_bytes_d}/${G_1}/${interval})*8")
        tx_dpdk_pkt_per_sec=$(bc <<< "scale=2; ${tx_vport_dpdk_unicast_packets_d}/${interval}")
	tx_dpdk_gb_per_sec=$(bc <<< "scale=2; (${tx_vport_dpdk_unicast_bytes_d}/${G_1}/${interval})*8")

        rx_all_pkt_per_sec=`max_number $rx_ipoib_pkt_per_sec $rx_rdma_pkt_per_sec $rx_dpdk_pkt_per_sec`
        rx_all_gb_per_sec=`max_number $rx_ipoib_gb_per_sec $rx_rdma_gb_per_sec $rx_dpdk_gb_per_sec`
        tx_all_pkt_per_sec=`max_number $tx_ipoib_pkt_per_sec $tx_rdma_pkt_per_sec $tx_dpdk_pkt_per_sec`
        tx_all_gb_per_sec=`max_number $tx_ipoib_gb_per_sec $tx_rdma_gb_per_sec $tx_dpdk_gb_per_sec`
}

OUTPUT_FORMAT_WIDTH=20

output_statistics() {
        if [ "x$protocol" == "xrdma" ]
        then
           printf "`date +%H:%M:%S` \t ${devname} \t ${protocol} \t ${rx_rdma_pkt_per_sec} \t ${tx_rdma_pkt_per_sec} \t ${rx_rdma_gb_per_sec} \t ${tx_rdma_gb_per_sec}\n" | expand -t ${OUTPUT_FORMAT_WIDTH}
        elif [ "x$protocol" == "xipoib" ]
        then
           printf "`date +%H:%M:%S` \t ${devname} \t ${protocol} \t ${rx_ipoib_pkt_per_sec} \t ${tx_ipoib_pkt_per_sec} \t ${rx_ipoib_gb_per_sec} \t ${tx_ipoib_gb_per_sec}\n" | expand -t ${OUTPUT_FORMAT_WIDTH}
        elif [ "x$protocol" == "xdpdk" ]
        then
           printf "`date +%H:%M:%S` \t ${devname} \t ${protocol} \t ${rx_dpdk_pkt_per_sec} \t ${tx_dpdk_pkt_per_sec} \t ${rx_dpdk_gb_per_sec} \t ${tx_dpdk_gb_per_sec}\n" | expand -t ${OUTPUT_FORMAT_WIDTH}
   	elif [ "x$protocol" == "xall" ]
        then
           printf "`date +%H:%M:%S` \t ${devname} \t rdma \t ${rx_rdma_pkt_per_sec} \t ${tx_rdma_pkt_per_sec} \t ${rx_rdma_gb_per_sec} \t ${tx_rdma_gb_per_sec}\n" | expand -t ${OUTPUT_FORMAT_WIDTH}
           printf "`date +%H:%M:%S` \t ${devname} \t ipoib \t ${rx_ipoib_pkt_per_sec} \t ${tx_ipoib_pkt_per_sec} \t ${rx_ipoib_gb_per_sec} \t ${tx_ipoib_gb_per_sec}\n" | expand -t ${OUTPUT_FORMAT_WIDTH}
	   printf "`date +%H:%M:%S` \t ${devname} \t dpdk \t ${rx_dpdk_pkt_per_sec} \t ${tx_dpdk_pkt_per_sec} \t ${rx_dpdk_gb_per_sec} \t ${tx_dpdk_gb_per_sec}\n" | expand -t ${OUTPUT_FORMAT_WIDTH}
           printf "`date +%H:%M:%S` \t ${devname} \t all \t ${rx_all_pkt_per_sec} \t ${tx_all_pkt_per_sec} \t ${rx_all_gb_per_sec} \t ${tx_all_gb_per_sec} \n" | expand -t ${OUTPUT_FORMAT_WIDTH}
           printf "\n"
        else
           printf "Not a valid protocol.\n" 
        fi
}

printf "`date +%H:%M:%S` \t IFACE \t Protocol \t rxpck/s \t txpck/s \t rxGb/s \t txGb/s\n" | expand -t ${OUTPUT_FORMAT_WIDTH}

while [ 1 ] 
do
	do_sampling_cal
	output_statistics
done
