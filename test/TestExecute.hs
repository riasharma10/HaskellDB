import Control.Concurrent.STM
import Control.Monad.State
import Control.Monad.IO.Class
import Execute
import Lib
import Types
import Test.HUnit
import Test.QuickCheck

import Data.IntMap (update)
import Data.Map qualified as Map
import GHC.IO.Device (IODevice (dup))

intVal :: Int
intVal = 123

col1 :: String 
col1 = "col1"

testTableName :: TableName 
testTableName = TableName "testTable"

initializeEmptyDB :: IO Database
initializeEmptyDB = do
  let initialDb = Database Map.empty Map.empty
  return initialDb

initializeEmptyTableDB :: IO Database
initializeEmptyTableDB = do
  let initialTable = createEmptyTable testTableName
  initialTableVar <- liftIO $ newTVarIO initialTable
  let initialDb = Database (Map.singleton testTableName initialTableVar) Map.empty
  return initialDb

initializeDBOneColumn :: IO Database
initializeDBOneColumn = do
  let colDefs = [ColumnDefinition (ColumnName col1) CellTypeInt]
  let initialTable = Table {tableName = testTableName, tableDefinition = colDefs, tableRows = Map.fromList [(PrimaryKey 0, Row $ Map.fromList [(ColumnName col1, CellInt intVal)])], tableNextPrimaryKey = PrimaryKey 1, tableIndices = []}
  initialTableVar <- liftIO $ newTVarIO initialTable
  let initialDb = Database (Map.singleton testTableName initialTableVar) Map.empty
  return initialDb

initializeDBOneColumnNoRows :: IO Database
initializeDBOneColumnNoRows = do
  let colDefs = [ColumnDefinition (ColumnName col1) CellTypeInt]
  let initialTable = Table {tableName = testTableName, tableDefinition = colDefs, tableRows = Map.empty, tableNextPrimaryKey = PrimaryKey 0, tableIndices = []}
  initialTableVar <- liftIO $ newTVarIO initialTable
  let initialDb = Database (Map.singleton testTableName initialTableVar) Map.empty
  return initialDb

test_selectTable :: Test
test_selectTable = TestCase $ do
  initialDb <- initializeDBOneColumn

  let cols = [ColumnName col1]
  let whereClauses = [WhereClause (ColumnName col1) (CellInt intVal)]

  let selectResult = runStateT (executeStatement $ StatementSelect cols testTableName whereClauses) initialDb
  (selectOutcome, _) <- liftIO selectResult

  case selectOutcome of
    Right rows -> assertBool "Should return at least one row" (not (null rows))
    Left _ -> assertFailure "Selection should succeed"

  let nonExistentTable = TableName "nonExistentTable"
  let failSelectResult = runStateT (executeStatement $ StatementSelect cols nonExistentTable whereClauses) initialDb
  (failSelectOutcome, _) <- liftIO failSelectResult

  assertEqual "Selecting from a non-existent table should fail" (Left TableDoesNotExist) failSelectOutcome



test_insertTable :: Test
test_insertTable = TestCase $ do
  db <- initializeDBOneColumnNoRows

  let row = Row $ Map.singleton (ColumnName col1) (CellInt intVal)
  let result = runStateT (executeStatement $ StatementInsert row testTableName) db
  (outcome, finalDb) <- liftIO result

  assertEqual "executeInsert should succeed" (Right []) outcome

  let table = Map.lookup testTableName (databaseTables finalDb)
  case table of
    Nothing -> assertFailure "Table should exist"
    Just tableVar -> do
      table <- liftIO $ readTVarIO tableVar
      let rows = tableRows table
      assertEqual "Table should have one row" 1 (Map.size rows)


-- TEST:
test_alterTable :: Test
test_alterTable = TestList [test_addColumn, test_dropColumn, test_renameColumn]

test_addColumn :: Test
test_addColumn = TestCase $ do
  initialDb <- initializeEmptyTableDB

  let newColumn = ColumnDefinition (ColumnName "newCol") CellTypeInt
  let alterAction = AddColumn newColumn

  let result = runStateT (executeStatement $ StatementAlter testTableName alterAction) initialDb
  (outcome, finalDb) <- liftIO result

  assertEqual "AddColumn should succeed" (Right []) outcome

  updatedTable <- liftIO $ readTVarIO (databaseTables finalDb Map.! testTableName)
  let columnExists = newColumn `elem` tableDefinition updatedTable
  assertBool "New column should be added to the table" columnExists

test_dropColumn :: Test
test_dropColumn = TestCase $ do
  initialDb <- initializeDBOneColumn

  let alterAction = DropColumn (ColumnName col1)

  let result = runStateT (executeStatement $ StatementAlter testTableName alterAction) initialDb
  (outcome, finalDb) <- liftIO result

  assertEqual "DropColumn should succeed" (Right []) outcome

  updatedTable <- liftIO $ readTVarIO (databaseTables finalDb Map.! testTableName)

  let columnExists = any ((== ColumnName col1) . columnDefinitionName) (tableDefinition updatedTable)
  assertBool "Column should not be in the table" (not columnExists)

test_renameColumn :: Test
test_renameColumn = TestCase $ do
  initialDb <- initializeDBOneColumn

  let renamedCol = ColumnName "newColName"
  let alterAction = RenameColumn (ColumnName col1) renamedCol

  let result = runStateT (executeStatement $ StatementAlter testTableName alterAction) initialDb
  (outcome, finalDb) <- liftIO result

  assertEqual "RenameColumn should succeed" (Right []) outcome

  updatedTable <- liftIO $ readTVarIO (databaseTables finalDb Map.! testTableName)

  let oldColumnExists = any ((== ColumnName col1) . columnDefinitionName) (tableDefinition updatedTable)
  let newColumnExists = any ((== renamedCol) . columnDefinitionName) (tableDefinition updatedTable)
  assertBool "Old Column should not be in the table" (not oldColumnExists)
  assertBool "new Column should be in the table" newColumnExists


test_createTable :: Test
test_createTable = TestCase $ do
  let createResult = runStateT (executeStatement $ StatementCreate tableName1 colDefs) initialDb
  (createOutcome, dbAfterCreate) <- liftIO createResult
  assertEqual "executeCreate should succeed" (Right []) createOutcome
  assertBool "Table should be created" (Map.member tableName1 (databaseTables dbAfterCreate))

  let duplicateCreateResult = runStateT (executeStatement $ StatementCreate tableName1 colDefs) dbAfterCreate
  (duplicateOutcome, _) <- liftIO duplicateCreateResult
  assertEqual "executeCreate should fail" (Left TableAlreadyExists) duplicateOutcome


test_createIndices :: Test
test_createIndices = TestList [test_createIndex, test_createIndexOnNonExistentTable]

test_createIndex :: Test
test_createIndex = TestCase $ do
  initialDb <- initializeDBOneColumnNoRows

  let indexName = IndexName "testIndex"
  let colNames = [ColumnName "col1"]
  let result = runStateT (executeStatement $ StatementCreateIndex indexName testTableName colNames) initialDb
  (outcome, finalDb) <- liftIO result

  assertEqual "executeCreateIndex should succeed" (Right []) outcome

  let indexExists = Map.member indexName (databaseIndices finalDb)
  assertBool "Index should be created" indexExists

  let tableVar = databaseTables finalDb Map.! testTableName
  table <- liftIO $ readTVarIO tableVar
  let indexNameInTable = indexName `elem` tableIndices table
  assertBool "Index name should be added to table" indexNameInTable

test_createIndexOnNonExistentTable :: Test
test_createIndexOnNonExistentTable = TestCase $ do
  initialDB <- initializeDBOneColumnNoRows

  let indexName = IndexName "testIndex"
  let colNames = [ColumnName "col1"]
  let nonExistentTable = TableName "nonExistentTable"
  let result = runStateT (executeStatement $ StatementCreateIndex indexName nonExistentTable colNames) initialDb
  (outcome, _) <- liftIO result

  assertEqual "executeCreateIndex should fail" (Left TableDoesNotExist) outcome


test_executeDrop :: Test
test_executeDrop = TestCase $ do
  initialTableVar <- liftIO $ newTVarIO $ createEmptyTable testTableName -- Assuming a function to create an empty table
  let initialDb = Database (Map.singleton testTableName initialTableVar) Map.empty

  let result = runStateT (executeStatement $ StatementDrop testTableName) initialDb
  (outcome, finalDb) <- liftIO result

  assertEqual "executeDrop should succeed" (Right []) outcome

  let tableExists = Map.member testTableName (databaseTables finalDb)
  assertBool "Table should be removed" (not tableExists)

test_executeDropIndex :: Test
test_executeDropIndex = TestCase $ do
  let indexName = IndexName "testIndex"
  let tableName = TableName "testTable"
  initialTableVar <- liftIO $ newTVarIO $ createEmptyTable tableName
  initialIndexVar <- liftIO $ newTVarIO $ createEmptyIndex indexName tableName
  let initialDb = Database (Map.singleton tableName initialTableVar) (Map.singleton indexName initialIndexVar)

  let result = runStateT (executeStatement $ StatementDropIndex indexName) initialDb
  (outcome, finalDb) <- liftIO result

  assertEqual "executeDrop should succeed" (Right []) outcome

  let indexExists = Map.member indexName (databaseIndices finalDb)
  assertBool "Index should be removed" (not indexExists)

test_executeStatement :: Test
test_executeStatement = TestList [test_selectTable, test_insertTable, test_alterTable, test_createTable, test_executeDrop, test_executeDropIndex]

runTests :: IO Counts
runTests = runTestTT test_executeStatement