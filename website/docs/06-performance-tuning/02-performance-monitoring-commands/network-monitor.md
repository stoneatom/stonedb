---
id: network-monitor
sidebar_position: 7.25
---
# Command for Network Monitoring

The topic describes the `netstat` command that is used to monitor network status.

Command output example: 
```shell
# netstat -i
Kernel Interface table
Iface      MTU    RX-OK RX-ERR RX-DRP RX-OVR    TX-OK TX-ERR TX-DRP TX-OVR Flg
docker0   1500    70712      0      0 0        131086      0      0      0 BMRU
eno1      1500 124457829      0 101794 0      57907365      0      0      0 BMRU
eno2      1500        0      0      0 0             0      0      0      0 BMU
eno3      1500        0      0      0 0             0      0      0      0 BMU
eno4      1500        0      0      0 0             0      0      0      0 BMU
enp130s0  1500        0      0      0 0             0      0      0      0 BMU
lo       65536     1123      0      0 0          1123      0      0      0 LRU
vethed74  1500      269      0      0 0           407      0      0      0 BMRU
```
Parameter description:

| **Parameter** | **Description** |
| --- | --- |
| Iface | The name of the network adapter. |
| MTU | The maximum transmission unit. Default value: 1500. |
| OK | The number of error-free packets. |
| ERR | The number of damaged packets. |
| DRP | The number of dropped packets. |
| OVR | The number of packets that exceeds the threshold. |
| Flg | The flag set for the interface. The value can be:<br />- **B**: A broadcast address is configured.<br />- **L**: The interface is a loopback device.<br />- **M**: All packets are received.<br />- **R**: The interface is running.<br />- **U**: The interface is active.<br /> |

:::info
**RX** indicates received. **TX** indicates sent.
:::