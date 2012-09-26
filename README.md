RsynchLargeFiles.py
===================

Intended to help automate the process of breaking up large files, transfering them to a remote machine and rebuilding them on the remote end.  This module is intended for networks where other methods of managing data transfers are not an option (such as more robust torrent applications).
 * Python version: 2.4.3
 * Intended operational environment:  Restricted. (Unable to add python modules outside of the standard package and no internet access available.)
 * External requirements: RSA/DSA keys configured between hosts.

As noted, this module is developed with a closed network and limited admin privileges are available.  Otherwise, I would have used an external library (such as Paramiko or Fabric) to perform all the SSH operations.

Current Status
==============

Splits, transfers and rebuilds the file on the remote end.  Things to do:
 * Review performance.
 * Updates of completion status seem clunky, at the moment.  Review how/when printProgress is called.
 * Some random rsync errors in the background.  Need to catch the exceptions and make sure there aren't associated fixes to make.
