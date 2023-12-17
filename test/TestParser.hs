module TestParser where

import Data.Map as Map
import StatementParser
import Test.HUnit
import Types
import Parser qualified as P

testParse :: Test
testParse =
  TestList
    [ test_columnName,
      test_whereClause,
      test_parseColumnNames,
      test_parseWhereClauses,
      test_selectQuery,
      test_insertQuery,
      test_alterQuery,
      test_dropQuery,
      test_dropIndexQuery,
      test_createTableQuery,
      test_createIndexQuery
    ]

test_columnName :: Test
test_columnName =
  TestList
    [ P.parse columnName "Users" ~?= Right (ColumnName "Users"),
      P.parse columnName "id, name" ~?= Right (ColumnName "id")
    ]

test_whereClause :: Test
test_whereClause =
  TestList
    [ P.parse whereClause "Users = 1" ~?= Right (WhereClause {whereClauseColumn = ColumnName "Users", whereClauseValue = CellInt 1}),
      P.parse whereClause "id = 1" ~?= Right (WhereClause {whereClauseColumn = ColumnName "id", whereClauseValue = CellInt 1}),
      P.parse whereClause "id = 1 AND user = rias" ~?= Right (WhereClause {whereClauseColumn = ColumnName "id", whereClauseValue = CellInt 1})
    ]

test_parseColumnNames :: Test
test_parseColumnNames =
  TestList
    [ parseColumnNames "id, name" ~?= Right [ColumnName "id", ColumnName "name"],
      parseColumnNames "id" ~?= Right [ColumnName "id"]
    ]

test_parseWhereClauses :: Test
test_parseWhereClauses =
  TestList
    [ parseWhereClauses "WHERE id = 1 AND user = \"rias\"" ~?= Right [WhereClause {whereClauseColumn = ColumnName "id", whereClauseValue = CellInt 1}, WhereClause {whereClauseColumn = ColumnName "user", whereClauseValue = CellString "rias"}],
      parseWhereClauses "WHERE id = 1" ~?= Right [WhereClause {whereClauseColumn = ColumnName "id", whereClauseValue = CellInt 1}]
    ]

test_selectQuery :: Test
test_selectQuery =
  TestList
    [ parseSQL "SELECT id, name FROM users WHERE id = 1" ~?= Right (StatementSelect [ColumnName "id", ColumnName "name"] (TableName "users") [WhereClause (ColumnName "id") (CellInt 1)]),
      parseSQL "SELECT id, name FROM users" ~?= Right (StatementSelect [ColumnName "id", ColumnName "name"] (TableName "users") []),
      parseSQL "SELECT id, name FROM users WHERE id = 1 AND name = \"rias\"" ~?= Right (StatementSelect [ColumnName "id", ColumnName "name"] (TableName "users") [WhereClause (ColumnName "id") (CellInt 1), WhereClause (ColumnName "name") (CellString "rias")]),
      parseSQL "SELECT id FROM users" ~?= Right (StatementSelect [ColumnName "id"] (TableName "users") []),
      parseSQL "select id FROM users" ~?= Left "No parses",
      parseSQL "SELECT id from users" ~?= Left "No parses"
    ]

test_insertQuery :: Test
test_insertQuery =
  TestList
    [ parseSQL "INSERT INTO users (num, name) VALUES 1, \"John Doe\"" ~?= Right (StatementInsert (Row $ Map.fromList [(ColumnName "num", CellInt 1), (ColumnName "name", CellString "John Doe")]) (TableName "users")),
      parseSQL "INSERT INTO users (num, name, isEligible) VALUES 1, \"John Doe\", TRUE" ~?= Right (StatementInsert (Row $ Map.fromList [(ColumnName "num", CellInt 1), (ColumnName "name", CellString "John Doe"), (ColumnName "isEligible", CellBool True)]) (TableName "users")),
      parseSQL "INSERT INTO users (num) VALUES 1" ~?= Right (StatementInsert (Row $ Map.fromList [(ColumnName "num", CellInt 1)]) (TableName "users")),
      parseSQL "INSERT INTO users VALUES" ~?= Left "No parses",
      parseSQL "INSERT INTO users" ~?= Left "No parses",
      parseSQL "INSERT INTO users (num, name, isEligible) VALUES 1, \"John Doe\" AND 1" ~?= Left "No parses"
    ]

test_alterQuery :: Test
test_alterQuery =
  TestList
    [ parseSQL "ALTER TABLE users ADD COLUMN id INT" ~?= Right (StatementAlter (TableName "users") (AddColumn (ColumnDefinition (ColumnName "id") CellTypeInt))),
      parseSQL "ALTER TABLE users DROP COLUMN id" ~?= Right (StatementAlter (TableName "users") (DropColumn (ColumnName "id"))),
      parseSQL "ALTER TABLE users RENAME COLUMN id TO newid" ~?= Right (StatementAlter (TableName "users") (RenameColumn (ColumnName "id") (ColumnName "newid"))),
      parseSQL "ALTER TABLE users ADD COLUMN id INT AND DROP id" ~?= Left "No parses",
      parseSQL "ALTER TABLE users" ~?= Left "No parses"
    ]

test_dropQuery :: Test
test_dropQuery =
  TestList
    [ parseSQL "DROP INDEX users" ~?= Right (StatementDropIndex (IndexName "users")),
      parseSQL "DROP INDEX users AND newusers" ~?= Left "No parses",
      parseSQL "DROP INDEX" ~?= Left "No parses"
    ]

test_dropIndexQuery :: Test
test_dropIndexQuery =
  TestList
    [ parseSQL "DROP TABLE users" ~?= Right (StatementDrop (TableName "users")),
      parseSQL "DROP TABLE users AND newusers" ~?= Left "No parses",
      parseSQL "DROP TABLE" ~?= Left "No parses"
    ]

test_createTableQuery :: Test
test_createTableQuery =
  TestList
    [ parseSQL "CREATE TABLE users (id INT, name STRING, isEligible BOOL)" ~?= Right (StatementCreate (TableName "users") [ColumnDefinition (ColumnName "id") CellTypeInt, ColumnDefinition (ColumnName "name") CellTypeString, ColumnDefinition (ColumnName "isEligible") CellTypeBool]),
      parseSQL "CREATE TABLE users (id INT, name STRING, isEligible BOOL) AND newusers (id INT, name STRING, isEligible BOOL)" ~?= Left "No parses",
      parseSQL "CREATE TABLE users" ~?= Left "No parses",
      parseSQL "CREATE TABLE users (id int, name string, isEligible bool)" ~?= Left "No parses"
    ]

test_createIndexQuery :: Test
test_createIndexQuery =
  TestList
    [ parseSQL "CREATE INDEX index ON users (id, name)" ~?= Right (StatementCreateIndex (IndexName "index") (TableName "users") [ColumnName "id", ColumnName "name"]),
      parseSQL "CREATE INDEX index ON users (id, name) AND newusers (id, name)" ~?= Left "No parses",
      parseSQL "CREATE INDEX index ON users" ~?= Left "No parses",
      parseSQL "CREATE INDEX index ON users (id)" ~?= Right (StatementCreateIndex (IndexName "index") (TableName "users") [ColumnName "id"])
    ]