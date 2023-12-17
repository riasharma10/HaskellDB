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
  StatementInsert row table -> executeInsert tdb row table
  StatementAlter table action -> executeAlter tdb table action
  StatementCreate table colDefs -> executeCreate tdb table colDefs
  StatementCreateIndex indexName table cols -> executeCreateIndex tdb indexName table cols
  StatementDrop table -> executeDrop tdb table
  StatementDropIndex indexName -> executeDropIndex tdb indexName

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

------------------- INSERT ------------------------
executeInsert :: DBRef -> Row -> TableName -> STM Response
executeInsert tdb row tableName = do
  db <- readTVar tdb
  case Map.lookup tableName (databaseTables db) of
    Nothing -> return $ Error TableDoesNotExist
    Just tableVar -> do
      table <- readTVar tableVar
      let newKey = tableNextPrimaryKey table
      let updatedRows = Map.insert newKey row (tableRows table)
      let updatedTable = table {tableRows = updatedRows, tableNextPrimaryKey = incrementPrimaryKey newKey}
      writeTVar tableVar updatedTable
      return $ Success []
  where
    incrementPrimaryKey :: PrimaryKey -> PrimaryKey
    incrementPrimaryKey (PrimaryKey k) = PrimaryKey (k + 1)

------------------- ALTER TABLE ------------------------
executeAlter :: DBRef -> TableName -> AlterAction -> STM Response
executeAlter tdb tableName action = do
  db <- readTVar tdb
  case Map.lookup tableName (databaseTables db) of
    Nothing -> return $ Error TableDoesNotExist
    Just tableVar -> do
      table <- readTVar tableVar
      newTable <- applyAlterAction table action
      writeTVar tableVar newTable
      return $ Success []

applyAlterAction :: Table -> AlterAction -> STM Table
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
executeCreate :: DBRef -> TableName -> [ColumnDefinition] -> STM Response
executeCreate tdb tableName colDefs = do
  db <- readTVar tdb
  case Map.lookup tableName (databaseTables db) of
    Just _ -> return $ Error TableAlreadyExists
    Nothing -> do
      let newTable =
            Table
              { tableName = tableName,
                tableDefinition = colDefs,
                tableRows = Map.empty,
                tableNextPrimaryKey = PrimaryKey 0,
                tableIndices = []
              }
      newTableVar <- newTVar newTable
      let updatedTables = Map.insert tableName newTableVar (databaseTables db)
      let updatedDb = db {databaseTables = updatedTables}
      writeTVar tdb updatedDb
      return $ Success []

tableName1 = TableName "newTable"

colDefs = [ColumnDefinition (ColumnName "col1") CellTypeInt]

executeCreateIndex :: DBRef -> IndexName -> TableName -> [ColumnName] -> STM Response
executeCreateIndex tdb indexName tableName colNames = do
  db <- readTVar tdb
  case Map.lookup tableName (databaseTables db) of
    Nothing -> return $ Error TableDoesNotExist
    Just tableVar -> do
      table <- readTVar tableVar
      case Map.lookup indexName (databaseIndices db) of
        Just _ -> return $ Error IndexAlreadyExists
        Nothing -> do
          let newIndex = Index {indexName = indexName, indexTable = tableName, indexColumns = colNames, indexData = Map.empty}
          newIndexVar <- newTVar newIndex
          let updatedIndices = Map.insert indexName newIndexVar (databaseIndices db)
          let updatedTable = addIndexNameToTable indexName table
          updatedTableVar <- newTVar updatedTable
          let updatedTables = Map.insert tableName updatedTableVar (databaseTables db)
          let updatedDb = db {databaseTables = updatedTables, databaseIndices = updatedIndices}
          writeTVar tdb updatedDb
          return $ Success []
          where
            addIndexNameToTable :: IndexName -> Table -> Table
            addIndexNameToTable indexName table =
              table {tableIndices = indexName : tableIndices table}

executeDrop :: DBRef -> TableName -> STM Response
executeDrop tdb tableName = do
  db <- readTVar tdb
  case Map.lookup tableName (databaseTables db) of
    Nothing -> return $ Error TableDoesNotExist
    Just _ -> do
      let updatedTables = Map.delete tableName (databaseTables db)
      let updatedDb = db {databaseTables = updatedTables}
      writeTVar tdb updatedDb
      return $ Success []

executeDropIndex :: DBRef -> IndexName -> STM Response
executeDropIndex tdb indexName = do
  db <- readTVar tdb
  case Map.lookup indexName (databaseIndices db) of
    Nothing -> return $ Error IndexDoesNotExist
    Just _ -> do
      updatedTables <- forM (Map.toList $ databaseTables db) $ \(tableName, tableVar) -> do
        table <- readTVar tableVar
        let updatedTable = removeIndexFromTable indexName table
        writeTVar tableVar updatedTable
        return (tableName, tableVar)
      let updatedIndices = Map.delete indexName (databaseIndices db)
      let updatedDb = db {databaseTables = Map.fromList updatedTables, databaseIndices = updatedIndices}
      writeTVar tdb updatedDb
      return $ Success []

removeIndexFromTable :: IndexName -> Table -> Table
removeIndexFromTable indexName table = table {tableIndices = filter (/= indexName) (tableIndices table)}