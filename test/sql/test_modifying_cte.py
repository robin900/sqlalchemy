from sqlalchemy.testing import fixtures
from sqlalchemy.testing import AssertsCompiledSQL, assert_raises_message
from sqlalchemy.sql import select, func, literal, text
from sqlalchemy.schema import Table, Column, MetaData
from sqlalchemy.sql.expression import exists, ModifyingCTE
from sqlalchemy.dialects import mssql
from sqlalchemy.engine import default
from sqlalchemy.exc import CompileError


class ModifyingCTETest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = 'postgresql'

    def test_nonrecursive(self):
        md = MetaData()
        orders = Table('orders', md,
                       Column('region', primary_key=True),
                       Column('amount'),
                       Column('product'),
                       Column('quantity')
                       )

        upsert = ModifyingCTE(orders.update()
                   .where(orders.c.region == 'Region1')
                   .values(amount=1.0,product='Product1',quantity=1)
                   .returning(*(orders.c._all_columns)), 'upsert'
                   )

        insert = orders.insert().from_select(
                     orders.c.keys(), 
                     select([literal(x) for x in ['Region1', 1.0, 'Product1', 1]]).where(exists(upsert.select()))
                     )

        self.assert_compile(
            insert,
            "WITH upsert AS (UPDATE orders SET amount = 1.0, " 
            "product = 'Product1', quantity = 1 WHERE region = 'Region1' " 
            "RETURNING region, amount, product, quantity) " 
            "INSERT INTO orders (region, amount, product, quantity) " 
            "SELECT ('Region1', 1.0, 'Product1', 1) WHERE NOT EXISTS " 
            "(SELECT * FROM upsert)"
        )
