RsynchLargeFiles.py
===================

Intended to help automate the process of breaking up large files, transfering them to a remote machine and rebuilding them on the remote end.  This module is intended for networks where other methods of managing data transfers are not an option (such as more robust torrent applications).


Current Status
==============

Splitting and data transfer complete successfully.  Things to do:
 * Review performance.
 * Updates of completion status seem clunky, at the moment.  Review how/when printProgress is called.
 * Add remote builder.
