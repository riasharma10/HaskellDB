module Main where

import Control.Concurrent.STM
import Control.Monad.IO.Class
import Control.Monad.State
import Data.IntMap (update)
import Data.Map qualified as Map
import Execute
import GHC.IO.Device (IODevice (dup))
import StatementParser
import Transaction
import Types

processStatement :: DBRef -> Statement -> IO ()
processStatement tdb statement = do
  outcome <- atomically $ executeStatement tdb statement
  print outcome

processTransaction :: DBRef -> Transaction -> IO ()
processTransaction tdb transaction = do
  outcome <- executeTransaction tdb transaction
  print outcome

mainLoop :: DBRef -> IO ()
mainLoop tdb = do
  putStr "HaskellDB> "
  command <- getLine
  case command of
    ":quit" -> putStrLn "Exiting Haskell Database Engine."
    _ -> do
      case parseTransaction command of
        Left err -> print err
        Right transaction -> processTransaction tdb transaction
      mainLoop tdb

main :: IO ()
main = do
  putStrLn "Welcome to the Haskell Database Engine!"
  dbRef <- atomically emptyDB
  mainLoop dbRef