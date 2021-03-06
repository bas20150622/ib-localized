from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.sqla import Base
from typing import Optional, List


class SQLASyncDB():
    """ Connect to sync (e.g. Postgres or SQLite) db usig sqlalchemy with dedicated engine and session """

    def __init__(self, dbname: str, echo: bool = False, drop: bool = False):
        """ Connect to db with url dbname, echo sql commands if echo==True, drop tables if drop==True
        in case of sqlite url: check_same_trhead = False """
        assert "///" in dbname, f"Error: missing dialect from dbname {dbname}"  # check for dbname
        self.dbname = dbname
        connect_args = {'check_same_thread': False} if "sqlite" in dbname.lower() else {
        }
        self.engine = create_engine(
            dbname, echo=echo, connect_args=connect_args)
        if drop:
            self.drop_all()
        self.create_all()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def create_all(self, models: Optional[List[Base]] = None):
        """
        Create all tables, or just the tables for the list of provided models
        :param models: list of sqla.model types such as User, UserLocation, Trade etc...
        """
        if models:
            _tables = [model.__table__ for model in models]
            Base.metadata.create_all(bind=self.engine, tables=_tables)
        else:
            Base.metadata.create_all(bind=self.engine)

    def drop_all(self, models: Optional[List[Base]] = None):
        """
        Drop all tables, or just the tables for the list of provided models
        :param models: list of sqla.model types such as User, UserLocation, Trade etc...
        """
        if models:
            _tables = [model.__table__ for model in models]
            Base.metadata.drop_all(bind=self.engine, tables=_tables)

        else:
            Base.metadata.drop_all(bind=self.engine)

    def closeDB(self):
        self.session.close()  # close session
        self.engine.dispose()  # close engine
        return 'db closed'
