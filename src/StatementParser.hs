module StatementParser where

import Control.Applicative
import Data.Char (isAlpha, isAlphaNum, isDigit)
import Data.Functor (($>))
import Data.Map as Map
import Parser (Parser)
import Parser qualified as P
import Types

alphaNum :: Parser Char
alphaNum = P.satisfy isAlphaNum

-- Basic parsers
identifier :: Parser String
identifier = some P.alpha <|> P.char '_' *> many alphaNum

parsedTableName :: Parser TableName
parsedTableName = TableName <$> identifier

columnName :: Parser ColumnName
columnName = ColumnName <$> identifier

-- >>> P.parse columnName "Users"

-- >>> P.parse columnName "id, name"

parsedCell :: Parser Cell
parsedCell = P.choice [cellInt, cellString, cellBool]

cellInt :: Parser Cell
cellInt = CellInt <$> P.int

cellString :: Parser Cell
cellString = CellString <$> (P.char '"' *> some (P.satisfy (/= '"')) <* P.char '"')

cellBool :: Parser Cell
cellBool = CellBool <$> (true <|> false)
  where
    true = P.string "TRUE" $> True
    false = P.string "FALSE" $> False

whereClause :: Parser WhereClause
whereClause =
  WhereClause
    <$> columnName
    <*> (P.space *> P.string "=" *> P.space *> parsedCell)

parseColumnNames :: String -> Either P.ParseError [ColumnName]
parseColumnNames = P.parse (P.sepBy columnName (P.char ',' <* P.space))

parseWhereClauses :: String -> Either P.ParseError [WhereClause]
parseWhereClauses = P.parse (P.string "WHERE" *> P.space *> whereClause `P.sepBy` (P.space *> P.string "AND" <* P.space))

selectParser :: Parser Statement
selectParser =
  StatementSelect
    <$> (P.string "SELECT" *> P.space *> columnNames <* P.space)
    <*> (P.string "FROM" *> P.space *> parsedTableName)
    <*> optionalWhereClause
  where
    columnNames = P.sepBy columnName (P.char ',' <* P.space)
    optionalWhereClause = (P.space *> P.string "WHERE" *> P.space *> whereClauses) <|> pure []
    whereClauses = whereClause `P.sepBy` (P.space *> P.string "AND" <* P.space)

insertParser =
  liftA3
    (\tableName columnNames cells -> StatementInsert (Row $ Map.fromList (zip columnNames cells)) tableName)
    (P.string "INSERT INTO" *> P.space *> parsedTableName <* P.space)
    (P.char '(' *> P.sepBy1 columnName (P.char ',' *> P.space) <* P.string ")" <* P.space <* P.string "VALUES" <* P.space)
    (P.sepBy1 parsedCell (P.char ',' *> P.space) <* P.eof)

dropParser :: Parser Statement
dropParser = StatementDrop <$> (P.string "DROP TABLE" *> P.space *> parsedTableName <* P.eof)

dropIndexParser :: Parser Statement
dropIndexParser = StatementDropIndex <$> (P.string "DROP INDEX" *> P.space *> indexName <* P.eof)
  where
    indexName = IndexName <$> identifier

createParser :: Parser Statement
createParser = StatementCreate <$> (P.string "CREATE TABLE" *> P.space *> parsedTableName) <*> (P.space *> P.char '(' *> P.sepBy columnDefinition (P.char ',' <* P.space) <* P.char ')' <* P.eof)
  where
    columnDefinition = ColumnDefinition <$> columnName <*> (P.space *> cellType)
    cellType = CellTypeInt <$ P.string "INT" <|> CellTypeString <$ P.string "STRING" <|> CellTypeBool <$ P.string "BOOL"

createIndexParser :: Parser Statement
createIndexParser = StatementCreateIndex <$> (P.string "CREATE INDEX" *> P.space *> indexName) <*> (P.space *> P.string "ON" *> P.space *> parsedTableName) <*> (P.space *> P.char '(' *> P.sepBy columnName (P.char ',' <* P.space) <* P.char ')' <* P.eof)
  where
    indexName = IndexName <$> identifier

parsedAlterAction :: Parser AlterAction
parsedAlterAction = P.choice [addColumn, dropColumn, renameColumn, modifyColumn]
  where
    addColumn = AddColumn <$> (P.string "ADD COLUMN" *> P.space *> columnDefinition)
    dropColumn = DropColumn <$> (P.string "DROP COLUMN" *> P.space *> columnName)
    renameColumn = RenameColumn <$> (P.string "RENAME COLUMN" *> P.space *> columnName) <*> (P.space *> P.string "TO" *> P.space *> columnName)
    modifyColumn = undefined
    columnDefinition = ColumnDefinition <$> columnName <*> (P.space *> cellType)
    cellType = CellTypeInt <$ P.string "INT" <|> CellTypeString <$ P.string "STRING" <|> CellTypeBool <$ P.string "BOOL"

alterParser :: Parser Statement
alterParser = StatementAlter <$> (P.string "ALTER TABLE" *> P.space *> parsedTableName) <*> (P.space *> parsedAlterAction <* P.eof)

-- Combine all statement parsers
statementParser :: Parser Statement
statementParser = P.choice [selectParser, insertParser, alterParser, dropParser, dropIndexParser, createParser, createIndexParser]

-- Example of how to use the parser
parseSQL :: String -> Either P.ParseError Statement
parseSQL = P.parse statementParser
