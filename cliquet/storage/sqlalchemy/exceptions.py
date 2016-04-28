"""Helper functions to translate database or sqlalchemy exceptions to cliquet standards
"""
import re

from cliquet import logger
from cliquet.storage.exceptions import BackendError, UnicityError


class DBError(object):

    searched_subclass_pattern = '{engine}Error'

    def __init__(self, error, collection, record):
        self.error = error
        self.collection = collection
        self.record = record

    @classmethod
    def get_class(cls, session, error, collection, record):
        for subclass in cls.__subclasses__():
            try:
                if cls.subclass_comparison(subclass, **cls.format_parameters(session=session, error=error)):
                    return subclass.get_class(session, error, collection, record).process_error()
            except UnicityError as e:
                raise
            except Exception as e:
                # probably engine not yet binded: log error and return standard parent class
                logger.exception('Error while fetching Error subclass: %s. Parameters were: %s, %s, %s',
                                 e, session, error, collection)
        return cls(error, collection, record).process_error()

    @classmethod
    def subclass_comparison(cls, candidate_subclass, **placehoders):
        return candidate_subclass.__name__ == cls.searched_subclass_pattern.format(**placehoders)

    @staticmethod
    def format_parameters(**kwargs):
        return {'engine': kwargs['session'].bind.engine.name}

    def process_error(self):
        logger.info('Generic error returned. Submitted data was %s, %s', self.error, self.collection)
        raise BackendError(original=self.collection, message='Validation error while creating object. Please report '
                                                             'this to support')


class postgresqlError(DBError):

    searched_subclass_pattern = '{engine}Error{error}'

    @staticmethod
    def format_parameters(**kwargs):
        return {'engine': kwargs['session'].bind.engine.name, 'error':kwargs['error'].orig.pgcode}


class postgresqlError23505(postgresqlError):

    regexp = r'\((.*?)\)'

    def process_error(self):
        field, _ = re.findall(self.regexp, self.error.orig.pgerror)
        raise UnicityError(field=field, record=self.record)


def process_unicity_error(error, session, obj, record):
    """Receive SQALAlchemy IntegrityError and according to the corresponding engine, inspect if
       this is a duplicated unique field (or not) and raise the correct cliquet error accordingly"""
    DBError.get_class(session=session, error=error, collection=obj, record=record)
