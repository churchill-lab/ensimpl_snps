# -*- coding: utf-8 -*-
import glob
import os
import sys

import ensimpl_snps.utils as utils

ENSIMPL_SNPS_DB_NAME = 'ensimpl_snps.*.db3'
ENSIMPL_SNPS_DBS = None
ENSIMPL_SNPS_DB_DICT = None



def get_ensimpl_snp_db(version, species):
    """Get the database based upon the `version` and `species` values which
    should represent the Ensembl reference number and species identifier.

    Args:
        version (int): The Ensembl version number.
        species (str): The short identifier of a species.

    Returns:
        str: The database file.

    Raises:
        ValueError: If unable to find the `version` and `species` combination
            or if `version` is not a number.

    Examples:
            >>> get_ensimpl_snps_db(91, 'Mm')
            'ensimpl_snps.91.Mm.db3'
    """
    try:
        return ENSIMPL_SNPS_DB_DICT['{}:{}'.format(int(version), species)]
    except Exception as e:
        raise ValueError('Unable to find version "{}" and species "{}"'.format(version, species))


def get_all_ensimpl_snps_dbs(directory):
    """Configure the list of ensimpl snp db files in `directory`.  This will
    set values for :data:`ENSIMPL_SNPS_DBS` and :data:`ENSIMPL_SNPS_DBS_DICT`.

    Args:
        directory (str): The directory path.
    """
    databases = glob.glob(os.path.join(directory, ENSIMPL_SNPS_DB_NAME))

    db_list = []
    db_dict = {}

    for db in databases:
        # db should be a string consisting of the following elements:
        # 'ensimpl_snps', version, species, 'db3'
        val = {
            'version': int(db.split('.')[1]),
            'species': db.split('.')[2],
            'db': db
        }
        db_list.append(val)

        # combined key will be 'version:species'
        combined_key = '{}:{}'.format(val['version'], val['species'])
        db_dict[combined_key] = val

    # sort the databases in descending order by version and than species for
    # readability in the API
    all_sorted_dbs = utils.multikeysort(db_list, ['-version', 'species'])

    global ENSIMPL_SNPS_DBS
    ENSIMPL_SNPS_DBS = all_sorted_dbs
    global ENSIMPL_SNPS_DB_DICT
    ENSIMPL_SNPS_DB_DICT = db_dict


def init(directory=None):
    """Initialize the configuration of the Ensimpl SNPs databases.

    NOTE: This method is referenced from the ``__init__.py`` in this module.

    Args:
        directory (str, optional): A directory that specifies where the ensimpl
            databases live. If None the environment variable
            ``ENSIMPL_SNPS_DIR`` will be used.
    """
    ensimpl_snps_dir = os.environ.get('ENSIMPL_SNPS_DIR', None)


    if directory:
        ensimpl_snps_dir = directory

    if not ensimpl_snps_dir:
        print('ENSIMPL_SNPS_DIR not configured in environment or directory '
              'was not supplied as an option')
        sys.exit()

    if not os.path.isabs(ensimpl_snps_dir):
        ensimpl_dir = os.path.abspath(os.path.join(os.getcwd(), ensimpl_snps_dir))
    else:
        ensimpl_dir = os.path.abspath(ensimpl_snps_dir)

    if not os.path.exists(ensimpl_dir):
        print('Specified ENSIMPL_SNPS_DIR does not exist: {}'.format(ensimpl_dir))
        sys.exit()

    if not os.path.isdir(ensimpl_dir):
        print('Specified ENSIMPL_SNPS_DIR is not a directory: {}'.format(ensimpl_dir))
        sys.exit()

    get_all_ensimpl_snps_dbs(ensimpl_dir)


