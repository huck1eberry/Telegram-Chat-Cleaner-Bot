from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm.scoping import scoped_session
from typing import Any, Callable, Optional, Protocol, Tuple, List
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DATETIME, func
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging
from sqlalchemy.orm.session import Session

from sqlalchemy.sql.sqltypes import DateTime

logger = logging.getLogger(__name__)

Base = declarative_base()

class MessageEntity(Base):
    """Represents message db-entity. Does not contain actual content - only message_id,
    chat_id and timestamp are stored"""
    
    __tablename__ = 'messages'

    message_id = Column(Integer, primary_key = True)
    chat_id = Column(Integer)
    timestamp = Column(DATETIME, default=func.now())

    def __repr__(self):
        return "<MessageEntity(message_id={message_id}, chat_id={chat_id}, timestamp={timestamp})>"\
        .format(message_id = self.message_id, chat_id = self.chat_id, timestamp = self.timestamp)


# Sidenotes about some common "exceptional" scenarios:
# sqlalchemy.exc.IntegrityError is triggerd whe the entity with the same primary key is presented.

class RepoOperation(Protocol):
        def __call__(self, repo: 'MessagesRepo', **kwargs) -> Any: ...

class MessagesRepo:
    """Basically, it is a DAO class representing messages DB-storage.
    Note: 'init_session' should be called before all other access-related methods are called."""
    
    def __init__(self, db_path: String):
        self.db_path = db_path
        self.session : Optional[Session] = None

    def init_session(self) -> None:
        engine = create_engine("sqlite:///{sqlite_filepath}".format(sqlite_filepath=self.db_path), echo=True)
        self.session = scoped_session(sessionmaker())
        self.session.configure(bind=engine)
        Base.metadata.create_all(engine)
    
    def close_session(self) -> None:
        self.session.close()

    def get_chat_messages(self, chat_id: int, min_timestamp: DateTime) -> List[MessageEntity]:
        return self.session.query(MessageEntity).order_by(MessageEntity.timestamp.desc()).filter(MessageEntity.chat_id == chat_id, MessageEntity.timestamp > min_timestamp).all()
    
    def delete_chat_messages(self, chat_id: int) -> None:
        self.session.query(MessageEntity).filter(MessageEntity.chat_id==chat_id).delete(synchronize_session=False)
        self.session.commit()
    
    def add_message(self, message: MessageEntity) -> None:
        # self.session.add(message) # TODO should we use add with the possibility to have catch IntegrityError here (in case if the message is already presented in the repo)?
        self.session.merge(message)
        self.session.commit()
    
    def remove_message(self, message: MessageEntity) -> None:
        self.session.delete(message)
        self.session.commit()
    
    def update_chat_id(self, original_chat_id: int, updated_chat_id: int) -> None:
        self.session.query(MessageEntity).filter(MessageEntity.chat_id==original_chat_id).update({MessageEntity.chat_id: updated_chat_id})
        self.session.commit()
    
    def get_all_chat_ids(self) -> List[int]:
        tupled_results = self.session.query(MessageEntity.chat_id.distinct()) 
        #sqlalchemy returns columns as a list of named tuples (like '[(result_1,), (result_2,)]), so we have to unpack it
        return [r for (r,) in tupled_results] 

    def remove_outdated_messages(self, date: DateTime) -> None:
        self.session.query(MessageEntity).filter(MessageEntity.timestamp < date).delete(synchronize_session=False)
        self.session.commit()

