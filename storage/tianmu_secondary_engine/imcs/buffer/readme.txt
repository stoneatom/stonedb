buffer, which is used for balancing the speed between memory and disk. There are some types buffer in IMCS.
The most important buffer is population buffer, which is used to store the chagnes occurred in innodb. Suppose
this following scenario. 1:) Update, A Update statement will changes some data in innodb, but IMSC should and
MUST know these changes, otherwise, IMCS is not consistent with InnoDB. When some changes made, then the
changes populate to population buffer. A background thread is launched at IMCS intialialization phase. That
background thread monitor this buffer, and apply the changes to IMCU to generate the latest version data. 
