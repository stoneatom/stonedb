---
id: network-monitor
sidebar_position: 7.25
---
# 网络监控

netstat命令能报告网络栈和接口统计信息，返回结果示例如下：
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

- **Iface**：网卡的名称
- **MTU**：最大传输单元，默认是1500
- **OK**：正确的数据包
- **ERR**：错误的数据包
- **DRP**：丢弃的数据包
- **OVR**：超限的数据包

**Flg标志位的含义**

- **B**：已经设置了一个广播地址
- **L**：该接口是一个loopback设备
- **M**：接收所有的数据包
- **R**：接口正在运行
- **U**：接口处于活动状态

注：RX表示接收，TX表示发送


