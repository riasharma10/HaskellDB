import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
import Data.Map qualified as Map
import Lib
import Test.HUnit (Counts, Test (TestCase, TestList), assertBool, assertEqual, assertFailure, runTestTT)
import Test.QuickCheck (Arbitrary (arbitrary), Gen, Property, quickCheck)
import Test.QuickCheck qualified as QC
import Test.QuickCheck.Monadic

main :: IO ()
main = do
  runTestTT $
    TestList
      [ testSelectTable,
        testInsertTable,
        testCreateTable,
        testExecuteDrop,
        testExecuteDropIndex
      ]
  quickCheck prop_createInsertSelect
  quickCheck prop_createInsertSelectWhere
  quickCheck prop_createInsertSelectWhereFalse
  quickCheck prop_createInsertSelectWhereTrueFalse
  quickCheck prop_selectNoCols
  quickCheck prop_createDropSelect
  quickCheck prop_createRenameSelectOld
  quickCheck prop_createRenameSelectNew
  quickCheck prop_createDeleteSelect
  quickCheck prop_createAddSelect
  testConcurrentTransactions
  putStrLn "Tests completed"

instance Arbitrary Cell where
  arbitrary :: Gen Cell
  arbitrary =
    QC.oneof
      [ CellInt <$> arbitrary,
        CellString <$> arbitrary,
        CellBool <$> arbitrary
      ]

prop_createInsertSelect = monadicIO $ do
  s :: String <- pick arbitrary
  res <- run $ atomically $ do
    tdb <- createDummyDb s
    executeStatement tdb (StatementSelect [ColumnName "column"] (TableName "table") [])
  assert (res == Success [createRow s])

createDummyDb :: String -> STM DBRef
createDummyDb s = do
  tdb <- emptyDB
  executeStatement
    tdb
    ( StatementCreate (TableName "table") [ColumnDefinition (ColumnName "column") CellTypeString]
    )
  executeStatement tdb (StatementInsert (createRow s) (TableName "table"))
  return tdb

createRow :: String -> Row
createRow s = Row $ Map.singleton (ColumnName "column") (CellString s)

prop_createInsertSelectWhere :: Property
prop_createInsertSelectWhere = monadicIO $ do
  s :: String <- pick arbitrary
  res <- run $ atomically $ do
    tdb <- createDummyDb s
    executeStatement tdb (StatementSelect [ColumnName "column"] (TableName "table") [WhereClause (ColumnName "column") (CellString s)])
  assert (res == Success [createRow s])

prop_createInsertSelectWhereFalse :: Property
prop_createInsertSelectWhereFalse = monadicIO $ do
  s1 :: String <- pick arbitrary
  s2 :: String <- pick arbitrary
  pre (s1 /= s2)
  res <- run $ atomically $ do
    tdb <- createDummyDb s1
    executeStatement tdb (StatementSelect [ColumnName "column"] (TableName "table") [WhereClause (ColumnName "column") (CellString s2)])
  assert (res == Success [])

prop_createInsertSelectWhereTrueFalse :: Property
prop_createInsertSelectWhereTrueFalse = monadicIO $ do
  s1 :: String <- pick arbitrary
  s2 :: String <- pick arbitrary
  pre (s1 /= s2)
  res <- run $ atomically $ do
    tdb <- createDummyDb s1
    executeStatement
      tdb
      ( StatementSelect
          [ColumnName "column"]
          (TableName "table")
          [WhereClause (ColumnName "column") (CellString s1), WhereClause (ColumnName "column") (CellString s2)]
      )
  assert (res == Success [])

prop_selectNoCols :: Property
prop_selectNoCols = monadicIO $ do
  s :: String <- pick arbitrary
  res <- run $ atomically $ do
    tdb <- emptyDB
    executeCreate tdb (TableName "table") []
    executeSelect tdb [ColumnName s] (TableName "table") []
  assert (res == Error ColumnDoesNotExist)

prop_createDropSelect :: Property
prop_createDropSelect = monadicIO $ do
  s :: String <- pick arbitrary
  res <- run $ atomically $ do
    tdb <- createDummyDb s
    executeStatement tdb (StatementDrop (TableName "table"))
    executeStatement tdb (StatementSelect [] (TableName "table") [])
  assert (res == Error TableDoesNotExist)

prop_createRenameSelectOld :: Property
prop_createRenameSelectOld = monadicIO $ do
  s1 :: String <- pick arbitrary
  s2 :: String <- pick arbitrary
  pre (s1 /= s2)
  res <- run $ atomically $ do
    tdb <- createAndRenameCol s1 s2
    executeStatement tdb (StatementSelect [ColumnName s1] (TableName "table") [])
  assert (res == Error ColumnDoesNotExist)

createAndRenameCol :: String -> String -> STM DBRef
createAndRenameCol s1 s2 = do
  tdb <- emptyDB
  executeStatement tdb (StatementCreate (TableName "table") [ColumnDefinition (ColumnName s1) CellTypeString])
  executeStatement tdb (StatementAlter (TableName "table") (RenameColumn (ColumnName s1) (ColumnName s2)))
  return tdb

prop_createRenameSelectNew :: Property
prop_createRenameSelectNew = monadicIO $ do
  s1 :: String <- pick arbitrary
  s2 :: String <- pick arbitrary
  pre (s1 /= s2)
  res <- run $ atomically $ do
    tdb <- createAndRenameCol s1 s2
    executeStatement tdb (StatementSelect [ColumnName s2] (TableName "table") [])
  assert (res == Success [])

prop_createDeleteSelect :: Property
prop_createDeleteSelect = monadicIO $ do
  s :: String <- pick arbitrary
  res <- run $ atomically $ do
    tdb <- emptyDB
    executeStatement tdb (StatementCreate (TableName "table") [ColumnDefinition (ColumnName s) CellTypeString])
    executeStatement tdb (StatementAlter (TableName "table") (DropColumn $ ColumnName s))
    executeStatement tdb (StatementSelect [ColumnName s] (TableName "table") [])
  assert (res == Error ColumnDoesNotExist)

prop_createAddSelect :: Property
prop_createAddSelect = monadicIO $ do
  s :: String <- pick arbitrary
  res <- run $ atomically $ do
    tdb <- emptyDB
    executeStatement tdb (StatementCreate (TableName "table") [])
    executeStatement tdb (StatementAlter (TableName "table") (AddColumn $ ColumnDefinition (ColumnName s) CellTypeString))
    executeStatement tdb (StatementSelect [ColumnName s] (TableName "table") [])
  assert (res == Success [])

testConcurrentTransactions = do
  tdb <- atomically emptyDB
  forkIO (quickCheck $ prop_createInsertSelectDrop tdb)
  forkIO (quickCheck $ prop_createInsertSelectDrop tdb)
  forkIO (quickCheck $ prop_createInsertSelectDrop tdb)
  forkIO (quickCheck $ prop_createInsertSelectDrop tdb)
  forkIO (quickCheck $ prop_createInsertSelectDrop tdb)
  forkIO (quickCheck $ prop_createInsertSelectDrop tdb)
  forkIO (quickCheck $ prop_createInsertSelectDrop tdb)
  forkIO (quickCheck $ prop_createInsertSelectDrop tdb)
  forkIO (quickCheck $ prop_createInsertSelectDrop tdb)
  forkIO (quickCheck $ prop_createInsertSelectDrop tdb)
  threadDelay (10 ^ 6) -- Hack to wait for threads to run

prop_createInsertSelectDrop :: DBRef -> Property
prop_createInsertSelectDrop tdb = monadicIO $ do
  s :: String <- pick arbitrary
  res <-
    run $
      executeTransaction
        tdb
        ( Atomic
            [ StatementCreate (TableName "table") [ColumnDefinition (ColumnName "column") CellTypeString],
              StatementInsert (createRow s) (TableName "table"),
              StatementSelect [ColumnName "column"] (TableName "table") [],
              StatementDrop (TableName "table")
            ]
        )
  assert (res == TransactionSuccess [Success [], Success [], Success [createRow s], Success []])

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