module Main where

import Control.Monad.IO.Class
import Control.Monad.State
import Data.IntMap (update)
import Data.Map qualified as Map
import Execute
import GHC.IO.Device (IODevice (dup))
import StatementParser
import Types

initializeEmptyDB :: IO Database
initializeEmptyDB = do
  let initialDb = Database Map.empty Map.empty
  return initialDb

processStatement :: Statement -> StateT Database IO ()
processStatement statement = do
  outcome <- executeStatement statement
  liftIO $ print outcome

mainLoop :: StateT Database IO ()
mainLoop = do
  liftIO $ putStr "HaskellDB> "
  command <- liftIO getLine
  case command of
    ":quit" -> liftIO $ putStrLn "Exiting Haskell Database Engine."
    _ -> do
      case parseSQL command of
        Left err -> liftIO $ print err
        Right statement -> processStatement statement
      mainLoop

main :: IO ()
main = do
  putStrLn "Welcome to the Haskell Database Engine!"
  db <- initializeEmptyDB
  void $ runStateT mainLoop db
