import datetime

from cliquet import logger
from colanderalchemy import SQLAlchemySchemaNode
from pyramid_sqlalchemy import BaseObject
from sqlalchemy import Column
from sqlalchemy import String, DateTime, Boolean, Integer, BigInteger
from sqlalchemy.ext.declarative import declarative_base

from cliquet.resource import UserResource, ResourceSchema
from cliquet.resource.schema import TimeStamp


class SQLABaseObject(object):

    _track_timestamp = True

    id = Column(Integer(), primary_key=True)
    parent_id = Column(String(), nullable=False, index=True, info={'colanderalchemy': {'exclude': True}})
    last_modified = Column(BigInteger(), nullable=False,)  #info={'colanderalchemy': {'typ': TimeStamp()}})
    deleted = Column(Boolean(), default=False, index=True, info={'colanderalchemy': {'exclude': True}})

    @property
    def is_timestamp_trackeable(self):
        """True if this object will be used to track the last time the collection it belongs to has been accessed"""
        return self._track_timestamp

    @property
    def last_modified_timestamp(self):
        return self.last_modified.replace(tzinfo=datetime.timezone.utc).timestamp()

    def deserialize(self):
        try:
            return self.__schema__.dictify(self)
        except AttributeError:
            logger.exception('Schema for collection %s has not been set', self.collection)
            raise Exception('Schema not set for model')

    def serialize(self, dict_, context=None):
        try:
            return self.__schema__.objectify(dict_, context)
        except AttributeError:
            logger.exception('Schema for collection %s has not been set', self.collection)
            raise Exception('Schema not set for model')


class SQLASchemaResource(SQLAlchemySchemaNode, ResourceSchema):

    def __init__(self, class_):
        super(SQLASchemaResource, self).__init__(class_=class_)


class SQLAUserResource(UserResource):

    def __init__(self, request, context=None):
        super(SQLAUserResource, self).__init__(request, context)
        self.model.storage.collection = self.appmodel
        setattr(self.model.storage.collection, '__schema__', SQLASchemaResource(self.appmodel))
        self.mapping = self.model.storage.collection.__schema__


Base = declarative_base(cls=(BaseObject, SQLABaseObject))
