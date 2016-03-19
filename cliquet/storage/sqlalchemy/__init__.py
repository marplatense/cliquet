import datetime
import re

from pyramid_sqlalchemy import BaseObject, Session, metadata
from sqlalchemy import Column
from sqlalchemy import DateTime, String, Integer
from sqlalchemy import select, func, and_, event
from sqlalchemy.sql import label
from sqlalchemy.exc import IntegrityError
import transaction

from cliquet import logger
from cliquet.utils import classname
from cliquet.storage import StorageBase
from cliquet.storage import DEFAULT_ID_FIELD, DEFAULT_MODIFIED_FIELD, DEFAULT_DELETED_FIELD
from cliquet.storage.exceptions import RecordNotFoundError, UnicityError, BackendError
from cliquet.storage.sqlalchemy.client import create_from_config
from cliquet.storage.sqlalchemy.generators import IntegerId


regexp_integrity_error_fields = r'\((.*?)\)'


class Deleted(BaseObject):
    __tablename__ = "deleted"
    id = Column(Integer(), nullable=False, primary_key=True)
    parent_id = Column(String(), nullable=False, primary_key=True)
    collection_id = Column(String(), nullable=False, primary_key=True)
    last_modified = Column(DateTime(), nullable=False)


class Timestamps(BaseObject):
    __tablename__ = "timestamps"
    parent_id = Column(String(), primary_key=True)
    collection_id = Column(String(), primary_key=True)
    last_modified = Column(DateTime, nullable=False)


@event.listens_for(Session, 'before_flush')
def populate_timestamps_table(session, flush_context, instances):
    for instance in session.new:
        if getattr(instance, 'is_timestamp_trackeable', False):
            timestamp = session.query(Timestamps).get([instance.parent_id, classname(instance)])
            timestamp.last_modified = datetime.datetime.utcnow()
            session.merge(timestamp)


class Storage(StorageBase):

    id_generator = IntegerId()

    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)

    def initialize_schema(self):
        """Create every necessary objects (like tables or indices) in the
        backend.

        This is excuted when the ``cliquet migrate`` command is ran.
        """
        self.flush()

    def flush(self, auth=None):
        """Remove **every** object from this storage.
        """
        metadata.drop_all()
        metadata.create_all()

    def collection_timestamp(self, collection_id, parent_id, auth=None):
        """Get the highest timestamp of every objects in this `collection_id` for
        this `parent_id`.

        .. note::

            This should take deleted objects into account.

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :returns: the latest timestamp of the collection.
        :rtype: int
        """
        tb = Timestamps.__table__
        qry = select([label('last_modified', func.max(tb.c.last_modified))]).where(and_(
                                                                                   tb.c.parent_id == parent_id,
                                                                                   tb.c.collection_id == collection_id))
        last_modified,  = Session.execute(qry).fetchone()
        if last_modified is None:
            last_modified =  datetime.datetime.utcnow()
            with transaction.manager:
                Session.add(Timestamps(parent_id=parent_id, collection_id=collection_id,
                                       last_modified=last_modified))
        return last_modified.replace(tzinfo=datetime.timezone.utc).timestamp()

    def create(self, collection_id, parent_id, record, id_generator=None,
               unique_fields=None, id_field=DEFAULT_ID_FIELD,
               modified_field=DEFAULT_MODIFIED_FIELD,
               auth=None):
        """Create the specified `object` in this `collection_id` for this `parent_id`.
        Assign the id to the object, using the attribute
        :attr:`cliquet.resource.Model.id_field`.

        .. note::

            This will update the collection timestamp.

        :raises: :exc:`cliquet.storage.exceptions.UnicityError`

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param dict record: the object to create.

        :returns: the newly created object.
        :rtype: dict
        """
        obj = self.collection(**record)
        obj.parent_id = parent_id
        setattr(obj, modified_field, datetime.datetime.utcnow())
        try:
            Session.add(obj)
            Session.flush()
        except IntegrityError as e:
            logger.exception('Object %s for collection %s raised %s', record, self.collection, e)
            if e.orig.pgcode == '23505':
                field, record = re.findall(regexp_integrity_error_fields, e.orig.pgerror)
                raise UnicityError(field=field, record=record)
            else:
                raise BackendError(original=self.collection, message='Validation error while creating object. '
                                                                     'Please report this to support')
        # TODO: store new timestamps date
        return obj.deserialize()

    def get(self, collection_id, parent_id, object_id,
            id_field=DEFAULT_ID_FIELD,
            modified_field=DEFAULT_MODIFIED_FIELD,
            auth=None):
        """Retrieve the object with specified `object_id`, or raise error
        if not found.

        :raises: :exc:`cliquet.storage.exceptions.RecordNotFoundError`

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param str object_id: unique identifier of the object

        :returns: the object object.
        :rtype: dict
        """
        obj = Session.query(self.collection).get(object_id)
        # TODO: verify permissions
        if obj is None or obj.deleted:
            raise RecordNotFoundError()
        return obj.deserialize()

    def update(self, collection_id, parent_id, object_id, object,
               unique_fields=None, id_field=DEFAULT_ID_FIELD,
               modified_field=DEFAULT_MODIFIED_FIELD,
               auth=None):
        """Overwrite the `object` with the specified `object_id`.

        If the specified id is not found, the object is created with the
        specified id.

        .. note::

            This will update the collection timestamp.

        :raises: :exc:`cliquet.storage.exceptions.UnicityError`

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param str object_id: unique identifier of the object
        :param dict object: the object to update or create.

        :returns: the updated object.
        :rtype: dict
        """
        obj = Session.query(self.collection).get(object_id)
        # TODO: verify permissions
        if obj is None:
            obj = self.create(collection_id=collection_id, parent_id=parent_id,
                              record=object, unique_fields=unique_fields,
                              id_field=id_field, modified_field=modified_field,
                              auth=None)
        else:
            for k, v in object.items():
                setattr(obj, k, v)
            obj = obj.deserialize()
        return obj

    def delete(self, collection_id, parent_id, object_id,
               with_deleted=True, id_field=DEFAULT_ID_FIELD,
               modified_field=DEFAULT_MODIFIED_FIELD,
               deleted_field=DEFAULT_DELETED_FIELD,
               auth=None, **kwargs):
        """Delete the object with specified `object_id`, and raise error
        if not found.

        Deleted objects must be removed from the database, but their ids and
        timestamps of deletion must be tracked for synchronization purposes.
        (See :meth:`cliquet.storage.StorageBase.get_all`)

        .. note::

            This will update the collection timestamp.

        :raises: :exc:`cliquet.storage.exceptions.RecordNotFoundError`

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param str object_id: unique identifier of the object
        :param bool with_deleted: track deleted record with a tombstone

        :returns: the deleted object, with minimal set of attributes.
        :rtype: dict
        """
        obj = Session.query(self.collection).get(object_id)
        # TODO: verify permissions
        if obj is None or getattr(obj, deleted_field):
            raise RecordNotFoundError()
        setattr(obj, deleted_field, True)
        setattr(obj, modified_field, datetime.datetime.utcnow())
        Session.add(Deleted(id=object_id, parent_id=parent_id,
                            collection_id=collection_id,
                            last_modified=getattr(obj, modified_field)))
        return obj.deserialize()

    def delete_all(self, collection_id, parent_id, filters=None,
                   with_deleted=True, id_field=DEFAULT_ID_FIELD,
                   modified_field=DEFAULT_MODIFIED_FIELD,
                   deleted_field=DEFAULT_DELETED_FIELD,
                   auth=None):
        """Delete all objects in this `collection_id` for this `parent_id`.

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param filters: Optionnally filter the objects to delete.
        :type filters: list of :class:`cliquet.storage.Filter`
        :param bool with_deleted: track deleted records with a tombstone

        :returns: the list of deleted objects, with minimal set of attributes.
        :rtype: list of dict
        """
        with transaction.manager:
            tb = self.collection.__table__
            qry = select([tb.c.id]).where(and_(tb.c.parent_id == parent_id, getattr(tb.c, deleted_field) == False))
            if filters:
                qry.append_whereclause(filters)
            rows = [{"id": every.id, "parent_id": parent_id, "collection_id": collection_id,
                     modified_field: datetime.datetime.utcnow()} for every in Session.execute(qry).fetchall()]
            Session.bulk_update_mappings(self.collection,
                                         [{"id": every['id'], deleted_field: True,
                                           modified_field: every[modified_field]} for every in rows])
            if with_deleted:
                Session.bulk_insert_mappings(Deleted, rows)
        return rows

    def purge_deleted(self, collection_id, parent_id, before=None,
                      id_field=DEFAULT_ID_FIELD,
                      modified_field=DEFAULT_MODIFIED_FIELD,
                      auth=None):
        """Delete all deleted object tombstones in this `collection_id`
        for this `parent_id`.

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param int before: Optionnal timestamp to limit deletion (exclusive)

        :returns: The number of deleted objects.
        :rtype: int

        """
        tb = Deleted.__table__
        rst = Session.execute(tb.delete().where(and_(tb.c.parent_id == parent_id, tb.c.collection_id == collection_id)))
        return rst

    def get_all(self, collection_id, parent_id, filters=None, sorting=None,
                pagination_rules=None, limit=None, include_deleted=False,
                id_field=DEFAULT_ID_FIELD,
                modified_field=DEFAULT_MODIFIED_FIELD,
                deleted_field=DEFAULT_DELETED_FIELD,
                auth=None):
        """Retrieve all objects in this `collection_id` for this `parent_id`.

        :param str collection_id: the collection id.
        :param str parent_id: the collection parent.

        :param filters: Optionally filter the objects by their attribute.
            Each filter in this list is a tuple of a field, a value and a
            comparison (see `cliquet.utils.COMPARISON`). All filters
            are combined using *AND*.
        :type filters: list of :class:`cliquet.storage.Filter`

        :param sorting: Optionnally sort the objects by attribute.
            Each sort instruction in this list refers to a field and a
            direction (negative means descending). All sort instructions are
            cumulative.
        :type sorting: list of :class:`cliquet.storage.Sort`

        :param pagination_rules: Optionnally paginate the list of objects.
            This list of rules aims to reduce the set of objects to the current
            page. A rule is a list of filters (see `filters` parameter),
            and all rules are combined using *OR*.
        :type pagination_rules: list of list of :class:`cliquet.storage.Filter`

        :param int limit: Optionnally limit the number of objects to be
            retrieved.

        :param bool include_deleted: Optionnally include the deleted objects
            that match the filters.

        :returns: the limited list of objects, and the total number of
            matching objects in the collection (deleted ones excluded).
        :rtype: tuple (list, integer)
        """
        qry = Session.query(self.collection).filter(self.collection.parent_id == parent_id)
        total_records = qry.count()
        if not include_deleted:
            qry =  qry.filter(getattr(self.collection, deleted_field) == False)
        if limit:
            qry = qry.limit(limit=limit)
        return ([every.deserialize() for every in qry.all()], total_records)


def load_from_config(config):
    settings = config.get_settings()
    max_fetch_size = int(settings['storage_max_fetch_size'])
    create_from_config(config, prefix='storage_')
    return Storage(max_fetch_size=max_fetch_size)
