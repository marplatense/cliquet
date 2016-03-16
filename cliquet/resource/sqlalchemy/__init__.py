import datetime

from pyramid_sqlalchemy import BaseObject
from sqlalchemy import Column
from sqlalchemy import String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base

from cliquet.resource.model import Model
from cliquet.resource import UserResource
import cliquet


class SQLABaseObject(object):

    id = Column(String(), primary_key=True)
    parent_id = Column(String(), nullable=False, index=True)
    last_modified = Column(DateTime, nullable=False)
    deleted = Column(Boolean, default=False, index=True)


    def deserialize(self):
        # TODO: to be replaced via mapping deserializer
        values = dict([every for every in self.__dict__.items() if not every[0].startswith('_')])
        values['last_modified'] = self.last_modified.replace(tzinfo=datetime.timezone.utc).timestamp()
        return values


class SQLAModel(Model):

    def __init__(self, storage, id_generator=None, collection_id='', parent_id='', auth=None):
        super(SQLAModel, self).__init__(storage=storage, id_generator=id_generator, collection_id=collection_id,
                                        parent_id=parent_id, auth=auth)


class SQLAUserResource(UserResource):

    def __init__(self, request, context=None):
        super(SQLAUserResource, self).__init__(request, context)
        self.model.storage.collection = self.appmodel


Base = declarative_base(cls=(BaseObject, SQLABaseObject))


