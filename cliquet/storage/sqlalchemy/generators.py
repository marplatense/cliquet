from cliquet.storage.generators import Generator


class IntegerId(Generator):

    regexp = r'^[0-9]+$'
    """Pattern for positive integers only"""

    def __call__(self):
        return '1'
