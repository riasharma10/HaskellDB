module Transaction where

import Control.Concurrent.STM
import Execute
import Types

executeTransaction :: DBRef -> Transaction -> IO TransactionResponse
executeTransaction tdb (Atomic statements) =
  atomically $
    orElse
      (TransactionSuccess <$> runTransaction tdb statements)
      (return TransactionError)

runTransaction :: DBRef -> [Statement] -> STM [Response]
runTransaction tdb [] = return []
runTransaction tdb (s : ss) = do
  res <- executeStatement tdb s
  case res of
    Error _ -> retry
    r -> do
      rs <- runTransaction tdb ss
      return (r : rs)
