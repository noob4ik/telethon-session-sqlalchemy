import datetime
from typing import Optional, Generator, Tuple, List

from sqlalchemy import Column, String, Integer, BigInteger, LargeBinary, Table
import sqlalchemy as sql

from telethon.sessions.memory import MemorySession, _SentFileType
from telethon import utils
from telethon.crypto import AuthKey
from telethon.tl.types import (
    InputPhoto, InputDocument, PeerUser, PeerChat, PeerChannel, updates
)

LATEST_VERSION = 2


class AlchemySessionContainer:
    def __init__(self, engine=None, connection: sql.engine.Connection = None,
                 table_metadata: sql.MetaData = None, table_prefix: str = "",
                 manage_tables: bool = True):
        if isinstance(engine, str):
            engine = sql.create_engine(engine)

        self.db_engine = engine
        if not connection:
            self.db = self.db_engine.connect()  # type: sql.engine.Connection
        else:
            self.db = connection  # type: sql.engine.Connection

        (self.version, self.sessions, self.entities, self.sent_files,
         self.update_state) = self.create_table_classes(table_prefix, table_metadata)

        if manage_tables:
            if not self.version.exists(self.db_engine):
                self.version.create(self.db_engine)
                self.sessions.create(self.db_engine)
                self.entities.create(self.db_engine)
                self.sent_files.create(self.db_engine)
                self.update_state.create(self.db_engine)
                self.db.execute(self.version.insert().values(version=LATEST_VERSION))
            else:
                self.check_and_upgrade_database()

    @staticmethod
    def create_table_classes(prefix: str, metadata: sql.MetaData
                             ) -> Tuple[Table, Table, Table, Table, Table]:
        version = Table("{prefix}version".format(prefix=prefix), metadata,
                        Column("version", Integer, primary_key=True))
        sessions = Table("{prefix}sessions".format(prefix=prefix), metadata,
                         Column("session_id", String(255), primary_key=True),
                         Column("dc_id", Integer, primary_key=True),
                         Column("server_address", String(255)),
                         Column("port", Integer),
                         Column("auth_key", LargeBinary))
        entities = Table("{prefix}entities".format(prefix=prefix), metadata,
                         Column("session_id", String(255), primary_key=True),
                         Column("id", BigInteger, primary_key=True),
                         Column("hash", BigInteger, nullable=False),
                         Column("username", String(32)),
                         Column("phone", BigInteger),
                         Column("name", String(255)))
        sent_files = Table("{prefix}sent_files".format(prefix=prefix), metadata,
                           Column("session_id", String(255), primary_key=True),
                           Column("md5_digest", LargeBinary, primary_key=True),
                           Column("file_size", Integer, primary_key=True),
                           Column("type", Integer, primary_key=True),
                           Column("id", BigInteger),
                           Column("hash", BigInteger))
        update_state = Table("{prefix}update_state".format(prefix=prefix), metadata,
                             Column("session_id", String(255), primary_key=True),
                             Column("entity_id", BigInteger, primary_key=True),
                             Column("pts", BigInteger),
                             Column("qts", BigInteger),
                             Column("date", BigInteger),
                             Column("seq", BigInteger),
                             Column("unread_count", Integer))
        return version, sessions, entities, sent_files, update_state

    def _add_column(self, table: Table, column: sql.Column) -> None:
        column_name = column.compile(dialect=self.db_engine.dialect)
        column_type = column.type.compile(self.db_engine.dialect)
        self.db.execute("ALTER TABLE {} ADD COLUMN {} {}".format(
            table.name, column_name, column_type))

    def check_and_upgrade_database(self) -> None:
        res = self.db.execute(sql.select([self.version.version]))  # type: sql.engine.ResultProxy
        row = res.fetchone()
        version = row[0][0] if row and row[0] else 1
        if version == LATEST_VERSION:
            return

        if version == 1:
            self.update_state.create(self.db_engine)
            version = 3
        elif version == 2:
            self._add_column(self.update_state, Column(type=Integer, name="unread_count"))

        self.db.execute(self.version.update(values=(LATEST_VERSION,)))

    def new_session(self, session_id):
        return AlchemySession(self, session_id)

    def list_sessions(self):
        return

    def save(self):
        pass


class AlchemySession(MemorySession):
    def __init__(self, container: AlchemySessionContainer, session_id: str):
        super().__init__()
        self.container = container  # type: AlchemySessionContainer
        self.db = container.db  # type: sql.engine.Connection
        self.version, self.sessions, self.entities, self.sent_files, self.update_state = (
            container.version, container.sessions, container.entities,
            container.sent_files, container.update_state)
        self.session_id = session_id  # type: str
        self._load_session()

    def select_all(self, table: Table) -> sql.engine.ResultProxy:
        return self.db.execute(sql.select([table]).where(table.c.session_id == self.session_id))

    def select_columns(self, table: Table, *args: List[Column]) -> sql.engine.ResultProxy:
        return self.db.execute(sql.select(args).where(table.c.session_id == self.session_id))

    def _load_session(self):
        row = self.select_all(self.sessions).fetchone()
        if row:
            _, self._dc_id, self._server_address, self._port, auth_key = row
            self._auth_key = AuthKey(data=auth_key)

    def clone(self, to_instance=None):
        return super().clone(MemorySession())

    def set_dc(self, dc_id, server_address, port):
        super().set_dc(dc_id, server_address, port)
        self._update_session_table()

        sessions = self._db_query(self.Session).all()
        session = sessions[0] if sessions else None
        if session and session.auth_key:
            self._auth_key = AuthKey(data=session.auth_key)
        else:
            self._auth_key = None

    @staticmethod
    def _row_to_update_state(row) -> Optional[updates.State]:
        if row:
            date = datetime.datetime.fromtimestamp(row.date, tz=datetime.timezone.utc)
            return updates.State(row.pts, row.qts, date, row.seq, row.unread_count)
        return None

    def iter_update_states(self) -> Generator[updates.State]:
        rows = self.UpdateState.query.filter(self.UpdateState.session_id == self.session_id).all()
        for row in rows:
            if row.entity_id != 0:
                state = self._row_to_update_state(row)
                if state:
                    yield row.entity_id, state

    def get_update_state(self, entity_id: int) -> updates.State:
        return self._row_to_update_state(self.select_all(self.update_state).fetchone())

    def set_update_state(self, entity_id: int, row: updates.State) -> None:
        if row:

            self.db.merge(self.UpdateState(session_id=self.session_id, entity_id=entity_id,
                                           pts=row.pts, qts=row.qts, date=row.date.timestamp(),
                                           seq=row.seq,
                                           unread_count=row.unread_count))
            self.save()

    @MemorySession.auth_key.setter
    def auth_key(self, value):
        self._auth_key = value
        self._update_session_table()

    def _update_session_table(self):
        self.Session.query.filter(
            self.Session.session_id == self.session_id).delete()
        new = self.Session(session_id=self.session_id, dc_id=self._dc_id,
                           server_address=self._server_address,
                           port=self._port,
                           auth_key=(self._auth_key.key
                                     if self._auth_key else b''))
        self.db.merge(new)

    def _db_query(self, dbclass, *args):
        return dbclass.query.filter(
            dbclass.session_id == self.session_id, *args
        )

    def save(self):
        self.container.save()

    def close(self):
        # Nothing to do here, connection is managed by AlchemySessionContainer.
        pass

    def delete(self):
        self._db_query(self.Session).delete()
        self._db_query(self.Entity).delete()
        self._db_query(self.SentFile).delete()

    def _entity_values_to_row(self, id, hash, username, phone, name):
        return self.Entity(session_id=self.session_id, id=id, hash=hash,
                           username=username, phone=phone, name=name)

    def process_entities(self, tlo):
        rows = self._entities_to_rows(tlo)
        if not rows:
            return

        for row in rows:
            self.db.merge(row)
        self.save()

    def get_entity_rows_by_phone(self, key):
        row = self._db_query(self.Entity,
                             self.Entity.phone == key).one_or_none()
        return (row.id, row.hash) if row else None

    def get_entity_rows_by_username(self, key):
        row = self._db_query(self.Entity,
                             self.Entity.username == key).one_or_none()
        return (row.id, row.hash) if row else None

    def get_entity_rows_by_name(self, key):
        row = self._db_query(self.Entity,
                             self.Entity.name == key).one_or_none()
        return (row.id, row.hash) if row else None

    def get_entity_rows_by_id(self, key, exact=True):
        if exact:
            query = self._db_query(self.Entity, self.Entity.id == key)
        else:
            ids = (
                utils.get_peer_id(PeerUser(key)),
                utils.get_peer_id(PeerChat(key)),
                utils.get_peer_id(PeerChannel(key))
            )
            query = self._db_query(self.Entity, self.Entity.id.in_(ids))

        row = query.one_or_none()
        return (row.id, row.hash) if row else None

    def get_file(self, md5_digest, file_size, cls):
        row = self._db_query(self.SentFile,
                             self.SentFile.md5_digest == md5_digest,
                             self.SentFile.file_size == file_size,
                             self.SentFile.type == _SentFileType.from_type(
                                 cls).value).one_or_none()
        return (row.id, row.hash) if row else None

    def cache_file(self, md5_digest, file_size, instance):
        if not isinstance(instance, (InputDocument, InputPhoto)):
            raise TypeError("Cannot cache {} instance".format(type(instance)))

        self.db.merge(
            self.SentFile(session_id=self.session_id, md5_digest=md5_digest,
                          type=_SentFileType.from_type(type(instance)).value,
                          id=instance.id, hash=instance.access_hash))
        self.save()
