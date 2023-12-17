import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
import Data.Map qualified as Map
import Lib
import Test.HUnit (Counts, Test (TestCase, TestList), assertBool, assertEqual, assertFailure, runTestTT)
import Test.QuickCheck (Arbitrary (arbitrary), Property, quickCheck)
import Test.QuickCheck.Monadic

main :: IO Counts
main = do
  runTestTT $ TestList [testSelectTable, testInsertTable, testCreateTable, testExecuteDrop, testExecuteDropIndex]

-- quickCheck prop_createInsertSelect

-- prop_createInsertSelect = monadicIO $ do
--   s :: String <- pick arbitrary
--   res <- run $ atomically $ do
--     db <- emptyDB
--     executeStatement
--       db
--       ( StatementCreate (TableName "table") [ColumnDefinition (ColumnName "column") CellTypeString]
--       )
--     executeStatement db (StatementInsert (createRow s) (TableName "table"))
--     executeStatement db (StatementSelect [ColumnName "column"] (TableName "table") [])
--   assert (res == Success [])

-- createRow :: String -> Row
-- createRow s = Row $ Map.singleton (ColumnName "column") (CellString s)

testSelectTable :: Test
testSelectTable = TestCase $ do
  let tableName = TableName "testTable"
  let colDefs = [ColumnDefinition (ColumnName "col1") CellTypeInt]
  let initialTable = Table {tableName = tableName, tableDefinition = colDefs, tableRows = Map.fromList [(PrimaryKey 0, Row $ Map.fromList [(ColumnName "col1", CellInt 123)])], tableNextPrimaryKey = PrimaryKey 1, tableIndices = []}
  initialTableVar <- liftIO $ newTVarIO initialTable
  let initialDb = Database (Map.singleton tableName initialTableVar) Map.empty
  tdb <- newTVarIO initialDb

  let cols = [ColumnName "col1"]
  let whereClauses = [WhereClause (ColumnName "col1") (CellInt 123)]

  res <- atomically $ executeSelect tdb cols tableName whereClauses

  case res of
    Success rows -> assertBool "Should return at least one row" (not (null rows))
    Error _ -> assertFailure "Selection should succeed"

  let nonExistentTable = TableName "nonExistentTable"
  failRes <- atomically $ executeSelect tdb cols nonExistentTable whereClauses
  assertEqual "Selecting from a non-existent table should fail" (Error TableDoesNotExist) failRes

testInsertTable :: Test
testInsertTable = TestCase $ do
  let tableName = TableName "testTable"
  let colDefs = [ColumnDefinition (ColumnName "col1") CellTypeInt]
  tableVar <-
    newTVarIO $
      Table
        { tableName = tableName,
          tableDefinition = colDefs,
          tableRows = Map.empty,
          tableNextPrimaryKey = PrimaryKey 0,
          tableIndices = []
        }
  tdb <- newTVarIO $ Database (Map.singleton tableName tableVar) Map.empty
  let row = Row $ Map.singleton (ColumnName "col1") (CellInt 1)
  res <- atomically $ executeInsert tdb row tableName
  assertEqual "executeInsert should succeed" (Success []) res
  db <- readTVarIO tdb
  let table = Map.lookup tableName (databaseTables db)
  case table of
    Nothing -> assertFailure "Table should exist"
    Just tableVar -> do
      table <- liftIO $ readTVarIO tableVar
      let rows = tableRows table
      assertEqual "Table should have one row" 1 (Map.size rows)

testCreateTable :: Test
testCreateTable = TestCase $ do
  tdb <- atomically emptyDB
  createRes <- atomically $ executeCreate tdb tableName1 colDefs
  assertEqual "executeCreate should succeed" (Success []) createRes
  db <- readTVarIO tdb
  assertBool "Table should be created" (Map.member tableName1 (databaseTables db))
  dupCreateRes <- atomically $ executeCreate tdb tableName1 colDefs
  assertEqual "executeCreate should fail" (Error TableAlreadyExists) dupCreateRes

testExecuteDrop :: Test
testExecuteDrop = TestCase $ do
  let tableName = TableName "testTable"
  tableVar <- newTVarIO $ createEmptyTable tableName -- Assuming a function to create an empty table
  tdb <- newTVarIO $ Database (Map.singleton tableName tableVar) Map.empty

  res <- atomically $ executeDrop tdb tableName
  assertEqual "executeDrop should succeed" (Success []) res
  db <- readTVarIO tdb
  let tableExists = Map.member tableName (databaseTables db)
  assertBool "Table should be removed" (not tableExists)

testExecuteDropIndex :: Test
testExecuteDropIndex = TestCase $ do
  let indexName = IndexName "testIndex"
  let tableName = TableName "testTable"
  tableVar <- newTVarIO $ createEmptyTable tableName
  indexVar <- newTVarIO $ createEmptyIndex indexName tableName
  tdb <- newTVarIO $ Database (Map.singleton tableName tableVar) (Map.singleton indexName indexVar)

  res <- atomically $ executeDropIndex tdb indexName
  assertEqual "executeDrop should succeed" (Success []) res
  db <- readTVarIO tdb
  let indexExists = Map.member indexName (databaseIndices db)
  assertBool "Index should be removed" (not indexExists)
