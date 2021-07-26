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


def loadWallet (log, args):
  """
  Loads the Arweave wallet specified in arguments.
  """

  wallet = arweave.Wallet (args.wallet)

  log.info (f"Loaded wallet {wallet.address}")
  log.info (f"Wallet balance is {wallet.balance} AR")

  return wallet


def performWrite (log, args, rpc):
  """
  Performs the "write" action (reading blocks as per the arguments from
  the blockchain daemon, and writing them to Arweave).
  """

  wallet = loadWallet (log, args)

  log.info (f"Writing blocks from height {args.fromHeight} to {args.toHeight}")

  ids = []
  for h in range (args.fromHeight, args.toHeight + 1):
    blkHash = rpc.getblockhash (h)
    hdr = rpc.getblockheader (blkHash)
    blk = rpc.getblock (blkHash, 0)
    blk = binascii.unhexlify (blk)

    tx = arweave.Transaction (wallet, data=blk)
    tx.add_tag ("App-Name", APP_NAME)
    tx.add_tag ("App-Version", APP_VERSION)
    tx.add_tag ("Blockchain", args.blockchain)
    tx.add_tag ("Block-Height", str (h))
    tx.add_tag ("Block-Hash", blkHash)
    if h > 0:
      tx.add_tag ("Previous-Hash", hdr["previousblockhash"])
    tx.sign ()
    tx.send ()

    log.info (f"Height {h}: {tx.id}")
    ids.append (tx.id)

  while ids:
    time.sleep (1)

    newIds = []
    for i in ids:
      tx = arweave.Transaction (wallet, id=i)
      tx.get_transaction ()

      status = tx.get_status ()
      if status == "PENDING":
        newIds.append (i)
      else:
        assert status["number_of_confirmations"] >= 1
        log.info (f"Confirmed: {i}")

    ids = newIds


def performRead (log, args, rpc):
  """
  Performs the "read" action, reading blocks from Arweave and passing
  them to the blockchain daemon.
  """

  wallet = loadWallet (log, args)

  query = {
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
    query = {
      "op": "and",
      "expr1": {
        "op": "equals",
        "expr1": "from",
        "expr2": args.address,
      },
      "expr2": query,
    }

  for h in range (args.fromHeight, args.toHeight + 1):
    fullQuery = {
      "op": "and",
      "expr1": {
        "op": "equals",
        "expr1": "Block-Height",
        "expr2": str (h),
      },
      "expr2": query,
    }
    txids = arweave.arql (wallet, fullQuery)

    for i in txids:
      tx = arweave.Transaction (wallet, id=i)
      tx.get_transaction ()
      tx.get_data ()

      try:
        rpc.submitblock (tx.data.hex ())
      except Exception as exc:
        log.error (exc)

    if rpc.getblockcount () >= h:
      log.info (f"Imported blocks at height {h}")
    else:
      log.error (f"Failed to find a block at height {h}")
      return


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
  parser.add_argument ("--from", required=True, type=int, dest="fromHeight",
                       help="Starting block height")
  parser.add_argument ("--to", required=True, type=int, dest="toHeight",
                       help="Ending block height")

  args = parser.parse_args ()

  valid = True

  if args.fromHeight < 0 or args.fromHeight > args.toHeight:
    valid = False

  if args.action == "write" and args.address:
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
