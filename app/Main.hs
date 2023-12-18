module Main where

import Control.Concurrent.STM
import Control.Monad.IO.Class
import Control.Monad.State
import Data.IntMap (update)
import Data.Map qualified as Map
import Execute
import GHC.IO.Device (IODevice (dup))
import StatementParser
import Types

processStatement :: DBRef -> Statement -> IO ()
processStatement tdb statement = do
  outcome <- atomically $ executeStatement tdb statement
  print outcome

mainLoop :: DBRef -> IO ()
mainLoop tdb = do
  putStr "HaskellDB> "
  command <- getLine
  case command of
    ":quit" -> putStrLn "Exiting Haskell Database Engine."
    _ -> do
      case parseSQL command of
        Left err -> print err
        Right statement -> processStatement tdb statement
      mainLoop tdb

main :: IO ()
main = do
  putStrLn "Welcome to the Haskell Database Engine!"
  dbRef <- atomically emptyDB
  mainLoop dbRef