#!/usr/bin/env python3

# Copyright (C) 2021 Daniel Kraft <d@domob.eu>
# Distributed under the MIT software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.

"""
Command-line utility to query for blocks from a Bitcoin-like blockchain
and store them into Arweave.
"""

import argparse
import binascii
import json
import logging
import sys
import time

import arweave
import jsonrpclib

APP_NAME = "ArBlockStore"
APP_VERSION = "1.0"

# Number of confirmations we want for data we are reading.
# This is mainly useful to check after writing some blocks.  Otherwise
# it won't matter, as data will be long confirmed anyway.
MIN_READ_CONFIRMATIONS = None


def loadWallet (log, args):
  """
  Loads the Arweave wallet specified in arguments.
  """

  wallet = arweave.Wallet (args.wallet)

  log.info (f"Loaded wallet {wallet.address}")
  log.info (f"Wallet balance is {wallet.balance} AR")

  return wallet


class BlockReader:
  """
  Helper class to query for block data on ArWeave.
  """

  def __init__ (self, log, wallet, args):
    self.log = log
    self.wallet = wallet

    self.baseQuery = {
      "op": "and",
      "expr1": {
        "op": "equals",
        "expr1": "App-Name",
        "expr2": APP_NAME,
      },
      "expr2": {
        "op": "equals",
        "expr1": "Blockchain",
        "expr2": args.blockchain,
      }
    }

    if args.address:
      log.info (f"Filtering transactions from {args.address}")
      self.baseQuery = {
        "op": "and",
        "expr1": {
          "op": "equals",
          "expr1": "from",
          "expr2": args.address,
        },
        "expr2": self.baseQuery,
      }

  def queryTxidsForBlock (self, h):
    """
    Queries for transactions with our filter and matching the
    given block height.  Returns just the txids, does not fetch
    the actual data for them yet.
    """

    query = {
      "op": "and",
      "expr1": {
        "op": "equals",
        "expr1": "Block-Height",
        "expr2": str (h),
      },
      "expr2": self.baseQuery,
    }

    return arweave.arql (self.wallet, query)


class BlockWriter:
  """
  Helper class for the "write" operation.
  """

  def __init__ (self, log, rpc, wallet):
    self.log = log
    self.rpc = rpc
    self.wallet = wallet

    # Transactions and block heights (as pairs) that have been
    # sent but are still pending.
    self.pending = []

  def generateTransactions (self, fromHeight, toHeight):
    """
    Generator that yields the transactions (not yet broadcast but
    otherwise ready) for storing blocks in the given range.
    """

    for h in range (fromHeight, toHeight + 1):
      blkHash = self.rpc.getblockhash (h)
      hdr = self.rpc.getblockheader (blkHash)
      blk = self.rpc.getblock (blkHash, 0)
      blk = binascii.unhexlify (blk)

      tx = arweave.Transaction (self.wallet, data=blk)
      tx.add_tag ("App-Name", APP_NAME)
      tx.add_tag ("App-Version", APP_VERSION)
      tx.add_tag ("Blockchain", args.blockchain)
      tx.add_tag ("Block-Height", str (h))
      tx.add_tag ("Block-Hash", blkHash)
      if h > 0:
        tx.add_tag ("Previous-Hash", hdr["previousblockhash"])
      tx.sign ()

      yield tx, h

  def checkPendings (self):
    """
    Queries for the status of each pending txid and removes those
    that have been confirmed.
    """

    newPending = []
    minHeight = None
    maxHeight = None

    for txid, oldTx, h in self.pending:
      tx = arweave.Transaction (self.wallet, id=txid)
      tx.get_transaction ()

      status = tx.get_status ()
      if status == "PENDING":
        oldTx.send ()
        newPending.append ((txid, oldTx, h))
        if minHeight is None or h < minHeight:
          minHeight = h
        if maxHeight is None or h > maxHeight:
          maxHeight = h
      else:
        assert status["number_of_confirmations"] >= 1
        self.log.info (f"Confirmed: {txid} for height {h}")

    self.pending = newPending
    cnt = len (self.pending)
    if cnt > 0:
      self.log.info (f"{cnt} tx pending, heights {minHeight} to {maxHeight}")

  def writeRange (self, fromHeight, toHeight, queueSize):
    """
    Runs the write operation in a given height range and with a maximum
    size of the pending queue (before new transactions are sent).
    """

    log.info (f"Writing blocks from height {fromHeight} to {toHeight}")

    moreTx = True
    generator = self.generateTransactions (fromHeight, toHeight)

    while moreTx or self.pending:
      while moreTx and len (self.pending) < queueSize:
        try:
          tx, h = next (generator)
          tx.send ()
          self.log.info (f"Height {h}: {tx.id}")
          self.pending.append ((tx.id, tx, h))
        except StopIteration:
          moreTx = False

      time.sleep (60)
      self.checkPendings ()


def performWrite (log, args, rpc):
  """
  Performs the "write" action (reading blocks as per the arguments from
  the blockchain daemon, and writing them to Arweave).
  """

  wallet = loadWallet (log, args)
  writer = BlockWriter (log, rpc, wallet)
  writer.writeRange (args.fromHeight, args.toHeight, args.pending_queue)


def performRead (log, args, rpc):
  """
  Performs the "read" action, reading blocks from Arweave and passing
  them to the blockchain daemon.
  """

  wallet = loadWallet (log, args)
  reader = BlockReader (log, wallet, args)

  if args.fromHeight == -1:
    args.fromHeight = rpc.getblockcount ()

  h = args.fromHeight
  while args.toHeight == -1 or h <= args.toHeight:
    txids = reader.queryTxidsForBlock (h)

    for i in txids:
      # It seems that sometimes there can be errors looking up
      # the transactions we received.  Just try to handle them
      # gracefully by ignoring.
      try:
        tx = arweave.Transaction (wallet, id=i)
        tx.get_transaction ()
        tx.get_data ()
      except Exception as exc:
        log.error (f"Error with {i}: {exc}")
        continue

      if MIN_READ_CONFIRMATIONS is not None:
        status = tx.get_status ()
        if status == "PENDING":
          continue
        if status["number_of_confirmations"] < MIN_READ_CONFIRMATIONS:
          continue

      try:
        rpc.submitblock (tx.data.hex ())
      except Exception as exc:
        log.error (exc)

    if rpc.getblockcount () >= h:
      log.info (f"Imported blocks at height {h}")
    else:
      log.error (f"Failed to find a block at height {h}")
      return

    h += 1


def setupLogging ():
  """
  Sets up the logging configuration we want to use on our logger
  and returns the instance.
  """

  logHandler = logging.StreamHandler (sys.stderr)
  logFmt = "%(asctime)s %(name)s (%(levelname)s): %(message)s"
  logHandler.setFormatter (logging.Formatter (logFmt))

  log = logging.getLogger ("arblockstore")
  log.setLevel (logging.INFO)
  log.addHandler (logHandler)

  logging.getLogger ("arweave").setLevel (logging.CRITICAL)

  return log


def parseArgs ():
  """
  Configures the argument parser and runs it to return the parsed
  arguments.
  """

  desc = "Stores and retrieves block data with ArWeave"

  parser = argparse.ArgumentParser (description=desc)
  parser.add_argument ("action", choices=["write", "read"],
                       help="The action to perform (write blocks to Arweave"
                            + " or read them and sync the local daemon")
  parser.add_argument ("--blockchain", required=True,
                       help="Name used to identify the blockchain in tags")
  parser.add_argument ("--rpc", required=True,
                       help="JSON-RPC endpoint of the blockchain daemon")
  parser.add_argument ("--wallet", required=True,
                       help="Arweave wallet file")
  parser.add_argument ("--address", default=None,
                       help="Filter for this address (when reading)")
  parser.add_argument ("--from", default=-1, type=int, dest="fromHeight",
                       help="Starting block height (defaults to current height when reading)")
  parser.add_argument ("--to", default=-1, type=int, dest="toHeight",
                       help="Ending block height (defaults to none when reading)")
  parser.add_argument ("--pending_queue", default=100, type=int,
                       help="Maximum number of pending transactions")

  args = parser.parse_args ()

  valid = True

  if args.fromHeight < -1:
    valid = False
  if args.toHeight != -1 and args.fromHeight > args.toHeight:
    valid = False

  if args.action == "write":
    if args.address:
      valid = False
    if args.fromHeight == -1 or args.toHeight == -1:
      valid = False

  if not valid:
    parser.print_help ()
    sys.exit (0)

  return args


if __name__ == "__main__":
  log = setupLogging ()
  args = parseArgs ()

  rpc = jsonrpclib.ServerProxy (args.rpc)
  cnt = rpc.getblockcount ()
  log.info (f"Local blockchain daemon has {cnt} blocks")

  if args.action == "write":
    performWrite (log, args, rpc)
  elif args.action == "read":
    performRead (log, args, rpc)
