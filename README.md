RsynchLargeFiles.py
===================

Intended to help automate the process of breaking up large files, transfering them to a remote machine and rebuilding them on the remote end.  This module is intended for networks where other methods of managing data transfers are not an option (such as more robust torrent applications).


Current Status
==============

Splits, transfers and rebuilds the file on the remote end.  Things to do:
 * Review performance.
 * Updates of completion status seem clunky, at the moment.  Review how/when printProgress is called.
 * Some random rsync errors in the background.  Need to catch the exceptions and make sure there aren't associated fixes to make.
