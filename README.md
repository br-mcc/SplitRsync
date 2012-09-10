RsynchLargeFiles.py
===================

Intended to help automate the process of breaking up large files, transfering them to a remote machine and rebuilding them on the remote end.  This module is intended for networks where other methods of managing data transfers are not an option (such as more robust torrent applications).


Current Status
==============

The module correctly splits files while producing a progress percentage during the split.  Exception handling was added to handle the different scenarios where some options are provided by the user.  The next step would be handling the rsync process itself.  
