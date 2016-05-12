import datetime

from colanderalchemy import SQLAlchemySchemaNode
from pyramid_sqlalchemy import BaseObject
from sqlalchemy import Column
from sqlalchemy import String, DateTime, Boolean, Integer
from sqlalchemy.ext.declarative import declarative_base

from cliquet.resource import UserResource


class SQLABaseObject(object):

    _track_timestamp = True

    id = Column(Integer(), primary_key=True)
    parent_id = Column(String(), nullable=False, index=True)
    last_modified = Column(DateTime(), nullable=False)
    deleted = Column(Boolean(), default=False, index=True)

    @property
    def is_timestamp_trackeable(self):
        """True if this object will be used to track the last time the collection it belongs to has been accessed"""
        return self._track_timestamp

    @property
    def last_modified_timestamp(self):
        return self.last_modified.replace(tzinfo=datetime.timezone.utc).timestamp()

    def deserialize(self, attributes):
        values = dict([(key, value) for (key, value) in self.__dict__.items() if key in attributes])
        values['last_modified'] = self.last_modified_timestamp
        return values


class SQLAUserResource(UserResource):

    def __init__(self, request, context=None):
        super(SQLAUserResource, self).__init__(request, context)
        self.model.storage.collection = self.appmodel
        self.mapping = SQLAlchemySchemaNode(self.model.storage.collection)
        self.model.storage.attributes = [every.name for every in self.mapping.children]


Base = declarative_base(cls=(BaseObject, SQLABaseObject))
