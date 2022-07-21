---
id: install-faq
sidebar_position: 10.1
---

# 安装部署FAQ

## 如何在KVM上运行StoneDB？
如果开发、测试环境是部署在虚拟机上的，AVX指令集必须开启，否则StoneDB无法安装。
检查命令如下所示：
```shell
cat /proc/cpuinfo | grep avx
```
若无返回结果，说明AVX指令集没有开启。
## 安装部署StoneDB需要哪些依赖文件？
不同的操作系统安装StoneDB，需要的依赖包是不一样的，将安装包解压出来后，可用如下方法检查需要的依赖包。
```shell
# cd /stonedb/install/bin
# ldd mysqld

linux-vdso.so.1 (0x00007ffd968d0000)
libpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x00007fb4bc0ad000)
librt.so.1 => /lib/x86_64-linux-gnu/librt.so.1 (0x00007fb4bc0a3000)
libssl.so.10 => /lib/libssl.so.10 (0x00007fb4bbe31000)
libcrypto.so.10 => /lib/libcrypto.so.10 (0x00007fb4bb9ce000)
libdl.so.2 => /lib/x86_64-linux-gnu/libdl.so.2 (0x00007fb4bb9c8000)
libncurses.so.5 => not found
libtinfo.so.5 => not found
libstdc++.so.6 => /lib/x86_64-linux-gnu/libstdc++.so.6 (0x00007fb4bb7e4000)
libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x00007fb4bb695000)
libgcc_s.so.1 => /lib/x86_64-linux-gnu/libgcc_s.so.1 (0x00007fb4bb67a000)
libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007fb4bb488000)
/lib64/ld-linux-x86-64.so.2 (0x00007fb4bc0da000)
libgssapi_krb5.so.2 => /lib/x86_64-linux-gnu/libgssapi_krb5.so.2 (0x00007fb4bb43b000)
libkrb5.so.3 => /lib/x86_64-linux-gnu/libkrb5.so.3 (0x00007fb4bb35c000)
libcom_err.so.2 => /lib/x86_64-linux-gnu/libcom_err.so.2 (0x00007fb4bb355000)
libk5crypto.so.3 => /lib/x86_64-linux-gnu/libk5crypto.so.3 (0x00007fb4bb324000)
libz.so.1 => /lib/x86_64-linux-gnu/libz.so.1 (0x00007fb4bb308000)
libkrb5support.so.0 => /lib/x86_64-linux-gnu/libkrb5support.so.0 (0x00007fb4bb2f9000)
libkeyutils.so.1 => /lib/x86_64-linux-gnu/libkeyutils.so.1 (0x00007fb4bb2f0000)
libresolv.so.2 => /lib/x86_64-linux-gnu/libresolv.so.2 (0x00007fb4bb2d4000)
# ldd mysql
linux-vdso.so.1 (0x00007ffd968d0000)
libpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x00007fb4bc0ad000)
librt.so.1 => /lib/x86_64-linux-gnu/librt.so.1 (0x00007fb4bc0a3000)
libssl.so.10 => /lib/libssl.so.10 (0x00007fb4bbe31000)
libcrypto.so.10 => /lib/libcrypto.so.10 (0x00007fb4bb9ce000)
libdl.so.2 => /lib/x86_64-linux-gnu/libdl.so.2 (0x00007fb4bb9c8000)
libncurses.so.5 => not found
libtinfo.so.5 => not found
libstdc++.so.6 => /lib/x86_64-linux-gnu/libstdc++.so.6 (0x00007fb4bb7e4000)
libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x00007fb4bb695000)
libgcc_s.so.1 => /lib/x86_64-linux-gnu/libgcc_s.so.1 (0x00007fb4bb67a000)
libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007fb4bb488000)
/lib64/ld-linux-x86-64.so.2 (0x00007fb4bc0da000)
libgssapi_krb5.so.2 => /lib/x86_64-linux-gnu/libgssapi_krb5.so.2 (0x00007fb4bb43b000)
libkrb5.so.3 => /lib/x86_64-linux-gnu/libkrb5.so.3 (0x00007fb4bb35c000)
libcom_err.so.2 => /lib/x86_64-linux-gnu/libcom_err.so.2 (0x00007fb4bb355000)
libk5crypto.so.3 => /lib/x86_64-linux-gnu/libk5crypto.so.3 (0x00007fb4bb324000)
libz.so.1 => /lib/x86_64-linux-gnu/libz.so.1 (0x00007fb4bb308000)
libkrb5support.so.0 => /lib/x86_64-linux-gnu/libkrb5support.so.0 (0x00007fb4bb2f9000)
libkeyutils.so.1 => /lib/x86_64-linux-gnu/libkeyutils.so.1 (0x00007fb4bb2f0000)
libresolv.so.2 => /lib/x86_64-linux-gnu/libresolv.so.2 (0x00007fb4bb2d4000)
```
检查结果中，"not found"表示缺少文件，需要安装对应的依赖包。
