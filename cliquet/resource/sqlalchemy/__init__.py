import datetime

from cliquet import logger
from colanderalchemy import setup_schema
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

    def deserialize(self):
        try:
            dict_ = self.__colanderalchemy__.dictify(self)
            dict_['last_modified'] = self.last_modified_timestamp
            return dict_
        except AttributeError:
            logger.exception('Schema for collection %s has not been set', self.collection)
            raise Exception('Schema not set for model')

    def serialize(self, dict_, context=None):
        try:
            return self.__colanderalchemy__.objectify(dict_, context)
        except AttributeError:
            logger.exception('Schema for collection %s has not been set', self.collection)
            raise Exception('Schema not set for model')


class SQLAUserResource(UserResource):

    def __init__(self, request, context=None):
        super(SQLAUserResource, self).__init__(request, context)
        self.model.storage.collection = self.appmodel
        setup_schema(None, self.model.storage.collection)
        self.mapping = self.model.storage.collection.__colanderalchemy__


Base = declarative_base(cls=(BaseObject, SQLABaseObject))
