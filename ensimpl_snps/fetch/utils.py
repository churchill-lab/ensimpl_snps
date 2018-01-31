# -*- coding: utf_8 -*-
import os
import re
import sqlite3

from collections import OrderedDict

import ensimpl_snps.utils as utils
import ensimpl_snps.db_config as db_config

LOG = utils.get_logger()

REGEX_REGION = re.compile("(CHR|)*\s*([0-9]{1,2}|X|Y|MT)\s*(-|:)?\s*(\d+)\s*(MB|M|K|)?\s*(-|:|)?\s*(\d+|)\s*(MB|M|K|)?", re.IGNORECASE)


class Region:
    """Encapsulates a genomic region.

    Attributes:
        chromosome (str): The chromosome name.
        start_position (int): The start position.
        end_position (int): The end position.
    """
    def __init__(self):
        """Initialization."""
        self.chromosome = ''
        self.start_position = None
        self.end_position = None

    def __str__(self):
        """Return string representing this region.

        Returns:
            str: In the format of chromosome:start_position-end_position.
        """
        return '{}:{}-{}'.format(self.chromosome,
                                 self.start_position,
                                 self.end_position)

    def __repr__(self):
        """Internal representation.

        Returns:
            dict: The keys being the attributes.
        """
        return {'chromosome': self.chromosome,
                'start_position': self.start_position,
                'end_position': self.end_position}


def connect_to_database(version, species):
    """Connect to the Ensimpl database.

    Args:
        version (int): The Ensembl version number.
        species (str): The Ensembl species identifier.

    Returns:
        a connection to the database
    """
    try:

        database = db_config.get_ensimpl_snp_db(version, species)['db']
        return sqlite3.connect(database)
    except Exception as e:
        LOG.error('Error connecting to database: {}'.format(str(e)))
        raise e


def get_tabix_file(version, species):
    """Get the tabix file.

    Args:
        version (int): The Ensembl version number.
        species (str): The Ensembl species identifier.

    Returns:
        str: A file location.
    """
    try:
        return db_config.get_ensimpl_snp_db(version, species)['vcf']
    except Exception as e:
        LOG.error('Error finding vcf file: {}'.format(str(e)))
        raise e


def nvl(value, default):
    """Returns `value` if value has a value, else `default`.

    Args:
        value: The value to evaluate.
        default: The default value.

    Returns:
        Either `value` or `default`.
    """
    return value if value else default


def nvli(value, default):
    """Returns `value` as an int if `value` can be converted, else `default`.

    Args:
        value: The value to evaluate and convert to an it.
        default: The default value.

    Returns:
        Either `value` or `default`.
    """
    ret = default
    if value:
        try:
            ret = int(value)
        except ValueError:
            pass
    return ret


def get_multiplier(factor):
    """Get multiplying factor.

    The factor value should be 'mb', 'm', or 'k' and the correct multiplier
    will be returned.

    Args:
        factor (str): One of 'mb', 'm', or 'k'.

    Returns:
        int: The multiplying value.
    """
    if factor:
        factor = factor.lower()

        if factor == 'mb':
            return 10000000
        elif factor == 'm':
            return 1000000
        elif factor == 'k':
            return 1000

    return 1


def str_to_region(location):
    """Parse a string into a genomic location.

    Args:
        location (str): The genomic location (range).

    Returns:
        Region: A region object.

    Raises:
        ValueError: If `location` is invalid.
    """
    if not location:
        raise ValueError('No location specified')

    valid_location = location.strip()

    if len(valid_location) <= 0:
        raise ValueError('Empty location')

    match = REGEX_REGION.match(valid_location)

    if not match:
        raise ValueError('Invalid location string')

    loc = Region()
    loc.chromosome = match.group(2)
    loc.start_position = match.group(4)
    loc.end_position = match.group(7)
    multiplier_one = match.group(5)
    multiplier_two = match.group(8)

    loc.start_position = int(loc.start_position)
    loc.end_position = int(loc.end_position)

    if multiplier_one:
        loc.start_position *= get_multiplier(multiplier_one)

    if multiplier_two:
        loc.end_position *= get_multiplier(multiplier_two)

    return loc

