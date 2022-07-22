---
id: install-faq
sidebar_position: 10.1
---

# Installation FAQ

## **What do I do if I want to run StoneDB on a KVM?**
If your development or test environment is deployed on a Kernel-based Virtual Machine (KVM), the AVX instruction set must be enabled. Otherwise, StoneDB cannot be installed.

You can run the following command to check whether the AVX instruction set is enabled.
```shell
cat /proc/cpuinfo | grep avx
```
If the command output is empty, the AVX instruction set is disabled.
## **What dependencies are required for StoneDB installation?**
The required dependencies vary with the OS used in the environment. You can run the following command to check the dependencies after decompressing the installation package.
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
If the command output contains **not found**, some dependencies are missing and need to be installed.
