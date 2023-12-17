module Execute where

import Control.Concurrent.STM
import Control.Monad.IO.Class
import Control.Monad.State
import Data.IntMap (update)
import Data.Map qualified as Map
import GHC.IO.Device (IODevice (dup))
import Test.HUnit
import Types

emptyDB :: STM DBRef
emptyDB = newTVar $ Database mempty mempty

initialDb = Database Map.empty Map.empty

defaultCellValue :: CellType -> Cell
defaultCellValue CellTypeInt = CellInt 0
defaultCellValue CellTypeString = CellString ""
defaultCellValue CellTypeBool = CellBool False

createEmptyTable :: TableName -> Table
createEmptyTable name =
  Table
    { tableName = name,
      tableDefinition = [],
      tableRows = Map.empty,
      tableNextPrimaryKey = PrimaryKey 0,
      tableIndices = []
    }

createEmptyIndex :: IndexName -> TableName -> Index
createEmptyIndex name tableName = Index {indexName = name, indexTable = tableName, indexColumns = [], indexData = Map.empty}

executeStatement :: DBRef -> Statement -> STM Response
executeStatement tdb statement = case statement of
  StatementSelect cols table whereClauses -> executeSelect tdb cols table whereClauses
  _ -> undefined

-- StatementInsert row table -> executeInsert row table
-- StatementAlter table action -> executeAlter table action
-- StatementCreate table colDefs -> executeCreate table colDefs
-- StatementCreateIndex indexName table cols -> executeCreateIndex indexName table cols
-- StatementDrop table -> executeDrop table
-- StatementDropIndex indexName -> executeDropIndex indexName

------------------- SELECT ------------------------
executeSelect :: DBRef -> [ColumnName] -> TableName -> [WhereClause] -> STM Response
executeSelect tdb cols tableName whereClauses = do
  db <- readTVar tdb
  case Map.lookup tableName (databaseTables db) of
    Nothing -> return $ Error TableDoesNotExist
    Just tableVar -> do
      table <- readTVar tableVar
      let filteredRows = filter (applyWhereClauses whereClauses) (Map.elems $ tableRows table)
      return $ Success $ map (selectColumns cols) filteredRows

applyWhereClauses :: [WhereClause] -> Row -> Bool
applyWhereClauses clauses (Row cellsMap) =
  all (\(WhereClause col val) -> maybe False (cellMatchesValue val) (Map.lookup col cellsMap)) clauses

cellMatchesValue :: Cell -> Cell -> Bool
cellMatchesValue cell value = case (cell, value) of
  (CellInt a, CellInt b) -> a == b
  (CellString a, CellString b) -> a == b
  (CellBool a, CellBool b) -> a == b
  _ -> False

selectColumns :: [ColumnName] -> Row -> Row
selectColumns cols (Row cellsMap) =
  Row $ Map.filterWithKey (\k _ -> k `elem` cols) cellsMap

-- TEST:
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

------------------- INSERT ------------------------
executeInsert :: (MonadDatabase m) => Row -> TableName -> m (Either StatementFailureType [Row])
executeInsert row tableName = do
  db <- get
  case Map.lookup tableName (databaseTables db) of
    Nothing -> return $ Left TableDoesNotExist
    Just tableVar -> do
      table <- liftIO $ readTVarIO tableVar
      let newKey = tableNextPrimaryKey table
      let updatedRows = Map.insert newKey row (tableRows table)
      let updatedTable = table {tableRows = updatedRows, tableNextPrimaryKey = incrementPrimaryKey newKey}
      liftIO $ atomically $ writeTVar tableVar updatedTable
      return $ Right []
  where
    incrementPrimaryKey :: PrimaryKey -> PrimaryKey
    incrementPrimaryKey (PrimaryKey k) = PrimaryKey (k + 1)

-- TEST:
testInsertTable :: Test
testInsertTable = TestCase $ do
  let tableName = TableName "testTable"
  let colDefs = [ColumnDefinition (ColumnName "col1") CellTypeInt]
  let tableVar = Table {tableName = tableName, tableDefinition = colDefs, tableRows = Map.empty, tableNextPrimaryKey = PrimaryKey 0, tableIndices = []}
  initialTableVar <- liftIO $ newTVarIO tableVar
  let db = Database (Map.singleton tableName initialTableVar) Map.empty

  let row = Row $ Map.singleton (ColumnName "col1") (CellInt 1)
  let result = runStateT (executeInsert row tableName) db
  (outcome, finalDb) <- liftIO result

  assertEqual "executeInsert should succeed" (Right []) outcome

  let table = Map.lookup tableName (databaseTables finalDb)
  case table of
    Nothing -> assertFailure "Table should exist"
    Just tableVar -> do
      table <- liftIO $ readTVarIO tableVar
      let rows = tableRows table
      assertEqual "Table should have one row" 1 (Map.size rows)

------------------- ALTER TABLE ------------------------
executeAlter :: (MonadDatabase m) => TableName -> AlterAction -> m (Either StatementFailureType [Row])
executeAlter tableName action = do
  db <- get
  case Map.lookup tableName (databaseTables db) of
    Nothing -> return $ Left TableDoesNotExist
    Just tableVar -> do
      table <- liftIO $ readTVarIO tableVar
      newTable <- liftIO $ applyAlterAction table action
      liftIO $ atomically $ writeTVar tableVar newTable
      return $ Right []

applyAlterAction :: Table -> AlterAction -> IO Table
applyAlterAction table action = case action of
  AddColumn colDef -> return $ addColumn table colDef
  DropColumn colName -> return $ dropColumn table colName
  RenameColumn oldName newName -> return $ renameColumn table oldName newName

addColumn :: Table -> ColumnDefinition -> Table
addColumn table colDef =
  table
    { tableDefinition = tableDefinition table ++ [colDef],
      tableRows = Map.map (addCellToRow colDef) (tableRows table)
    }

dropColumn :: Table -> ColumnName -> Table
dropColumn table colName =
  table
    { tableDefinition = filter ((/= colName) . columnDefinitionName) (tableDefinition table),
      tableRows = Map.map (removeCellFromRow colName) (tableRows table)
    }

renameColumn :: Table -> ColumnName -> ColumnName -> Table
renameColumn table oldName newName =
  table
    { tableDefinition = map (updateColDefName oldName newName) (tableDefinition table),
      tableRows = Map.map (renameCellInRow oldName newName) (tableRows table)
    }
  where
    updateColDefName :: ColumnName -> ColumnName -> ColumnDefinition -> ColumnDefinition
    updateColDefName oldName newName colDef
      | columnDefinitionName colDef == oldName = colDef {columnDefinitionName = newName}
      | otherwise = colDef

-- Need to update all column changes in the Row type as well:
addCellToRow :: ColumnDefinition -> Row -> Row
addCellToRow (ColumnDefinition colName colType) (Row rowMap) =
  let defaultCell = defaultCellValue colType
   in Row $
        Map.insert
          colName
          defaultCell
          rowMap

removeCellFromRow :: ColumnName -> Row -> Row
removeCellFromRow colName (Row rowMap) =
  Row $ Map.delete colName rowMap

renameCellInRow :: ColumnName -> ColumnName -> Row -> Row
renameCellInRow oldName newName (Row rowMap) =
  case Map.lookup oldName rowMap of
    Just cell -> Row $ Map.insert newName cell $ Map.delete oldName rowMap
    Nothing -> Row rowMap

------------------- CREATE TABLE ------------------------
executeCreate :: (MonadDatabase m) => TableName -> [ColumnDefinition] -> m (Either StatementFailureType [Row])
executeCreate tableName colDefs = do
  db <- get
  case Map.lookup tableName (databaseTables db) of
    Just _ -> return $ Left TableAlreadyExists
    Nothing -> do
      let newTable =
            Table
              { tableName = tableName,
                tableDefinition = colDefs,
                tableRows = Map.empty,
                tableNextPrimaryKey = PrimaryKey 0,
                tableIndices = []
              }
      newTableVar <- liftIO $ newTVarIO newTable
      let updatedTables = Map.insert tableName newTableVar (databaseTables db)
      let updatedDb = db {databaseTables = updatedTables}
      put updatedDb
      return $ Right []

tableName1 = TableName "newTable"

colDefs = [ColumnDefinition (ColumnName "col1") CellTypeInt]

testCreateTable :: Test
testCreateTable = TestCase $ do
  let createResult = runStateT (executeCreate tableName1 colDefs) initialDb
  (createOutcome, dbAfterCreate) <- liftIO createResult
  --   createOutcome ~?= Right []
  --   Map.member tableName (databaseTables dbAfterCreate) ~?= True
  assertEqual "executeCreate should succeed" (Right []) createOutcome
  assertBool "Table should be created" (Map.member tableName1 (databaseTables dbAfterCreate))

  let duplicateCreateResult = runStateT (executeCreate tableName1 colDefs) dbAfterCreate
  (duplicateOutcome, _) <- liftIO duplicateCreateResult
  assertEqual "executeCreate should fail" (Left TableAlreadyExists) duplicateOutcome

--   duplicateOutcome ~?= Left TableAlreadyExists

------------------- CREATE INDEX ------------------------
executeCreateIndex :: (MonadDatabase m) => IndexName -> TableName -> [ColumnName] -> m (Either StatementFailureType [Row])
executeCreateIndex indexName tableName colNames = do
  db <- get
  case Map.lookup tableName (databaseTables db) of
    Nothing -> return $ Left TableDoesNotExist
    Just tableVar -> do
      table <- liftIO $ readTVarIO tableVar
      case Map.lookup indexName (databaseIndices db) of
        Just _ -> return $ Left IndexAlreadyExists
        Nothing -> do
          let newIndex = Index {indexName = indexName, indexTable = tableName, indexColumns = colNames, indexData = Map.empty}
          newIndexVar <- liftIO $ newTVarIO newIndex
          let updatedIndices = Map.insert indexName newIndexVar (databaseIndices db)
          let updatedTable = addIndexNameToTable indexName table
          updatedTableVar <- liftIO $ newTVarIO updatedTable
          let updatedTables = Map.insert tableName updatedTableVar (databaseTables db)
          let updatedDb = db {databaseTables = updatedTables, databaseIndices = updatedIndices}
          put updatedDb
          return $ Right []
          where
            addIndexNameToTable :: IndexName -> Table -> Table
            addIndexNameToTable indexName table =
              table {tableIndices = indexName : tableIndices table}

------------------- DROP TABLE ------------------------
executeDrop :: (MonadDatabase m) => TableName -> m (Either StatementFailureType [Row])
executeDrop tableName = do
  db <- get
  case Map.lookup tableName (databaseTables db) of
    Nothing -> return $ Left TableDoesNotExist
    Just _ -> do
      let updatedTables = Map.delete tableName (databaseTables db)
      let updatedDb = db {databaseTables = updatedTables}
      put updatedDb
      return $ Right []

-- TEST:
testExecuteDrop :: Test
testExecuteDrop = TestCase $ do
  let tableName = TableName "testTable"
  initialTableVar <- liftIO $ newTVarIO $ createEmptyTable tableName -- Assuming a function to create an empty table
  let initialDb = Database (Map.singleton tableName initialTableVar) Map.empty

  let result = runStateT (executeDrop tableName) initialDb
  (outcome, finalDb) <- liftIO result

  assertEqual "executeDrop should succeed" (Right []) outcome

  let tableExists = Map.member tableName (databaseTables finalDb)
  assertBool "Table should be removed" (not tableExists)

------------------- DROP INDEX ------------------------
executeDropIndex :: (MonadDatabase m) => IndexName -> m (Either StatementFailureType [Row])
executeDropIndex indexName = do
  db <- get
  case Map.lookup indexName (databaseIndices db) of
    Nothing -> return $ Left IndexDoesNotExist
    Just _ -> do
      updatedTables <- liftIO $ atomically $ do
        forM (Map.toList $ databaseTables db) $ \(tableName, tableVar) -> do
          table <- readTVar tableVar
          let updatedTable = removeIndexFromTable indexName table
          writeTVar tableVar updatedTable
          return (tableName, tableVar)
      let updatedIndices = Map.delete indexName (databaseIndices db)
      let updatedDb = db {databaseTables = Map.fromList updatedTables, databaseIndices = updatedIndices}
      put updatedDb
      return $ Right []

removeIndexFromTable :: IndexName -> Table -> Table
removeIndexFromTable indexName table = table {tableIndices = filter (/= indexName) (tableIndices table)}

-- TEST:
testExecuteDropIndex :: Test
testExecuteDropIndex = TestCase $ do
  let indexName = IndexName "testIndex"
  let tableName = TableName "testTable"
  initialTableVar <- liftIO $ newTVarIO $ createEmptyTable tableName
  initialIndexVar <- liftIO $ newTVarIO $ createEmptyIndex indexName tableName
  let initialDb = Database (Map.singleton tableName initialTableVar) (Map.singleton indexName initialIndexVar)

  let result = runStateT (executeDropIndex indexName) initialDb
  (outcome, finalDb) <- liftIO result

  -- TestList
  -- [ outcome ~?= Right [],
  --   Map.member indexName (databaseIndices finalDb) ~?= False
  -- ]

  assertEqual "executeDrop should succeed" (Right []) outcome

  let indexExists = Map.member indexName (databaseIndices finalDb)
  assertBool "Index should be removed" (not indexExists)

testExecuteStatement :: Test
testExecuteStatement = TestList [testSelectTable, testInsertTable, testCreateTable, testExecuteDrop, testExecuteDropIndex]

runTests :: IO Counts
runTests = runTestTT testExecuteStatement