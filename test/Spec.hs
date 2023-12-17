import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
import Data.Map qualified as Map
import Lib
import Test.HUnit (Counts, Test (TestList), runTestTT)
import Test.QuickCheck (Arbitrary (arbitrary), Property, quickCheck)
import Test.QuickCheck.Monadic
import TestParser

main :: IO ()
main = do
  runTestTT testParse
  quickCheck prop_createInsertSelect

prop_createInsertSelect = monadicIO $ do
  s :: String <- pick arbitrary
  res <- run $ atomically $ do
    db <- emptyDB
    executeStatement
      db
      ( StatementCreate (TableName "table") [ColumnDefinition (ColumnName "column") CellTypeString]
      )
    executeStatement db (StatementInsert (createRow s) (TableName "table"))
    executeStatement db (StatementSelect [ColumnName "column"] (TableName "table") [])
  assert (res == Success [])

createRow :: String -> Row
createRow s = Row $ Map.singleton (ColumnName "column") (CellString s)