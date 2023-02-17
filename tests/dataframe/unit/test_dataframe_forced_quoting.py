import unittest
import sqlglot
from sqlglot.dataframe.sql.session import SparkSession
from sqlglot.expressions import Column
from sqlglot.dataframe.sql.functions import col

class TestDataframeForcedQuoting(unittest.TestCase):
    def test_forced_quoting(self):
        forced_quote_table_name = '"Test"'

        sqlglot.schema.add_table(forced_quote_table_name, dict(name="int"))

        _col = Column(
            this='name',
            table=forced_quote_table_name
        )

        name_column = col(_col)

        df = SparkSession().table(forced_quote_table_name)# .select(name_column).where(name_column.__eq__(1))

        # df = df.select(name_column)

        sql = df.sql(dialect='snowflake', optimize=True)

        print(sql)

        assert sql == ['SELECT "Test"."name" AS "name" FROM "Test" AS "Test"']

        df = df.select(name_column)

        sql = df.sql(dialect='snowflake', optimize=True)

        print(sql)

        assert sql == ['SELECT "Test"."name" FROM "Test"']
