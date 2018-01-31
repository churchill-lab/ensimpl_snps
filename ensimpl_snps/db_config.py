# -*- coding: utf-8 -*-
import glob
import os
import sys

import ensimpl_snps.utils as utils

ENSIMPL_SNPS_DB_NAME = 'ensimpl_snps.*.db3'
ENSIMPL_SNPS_DBS = None
ENSIMPL_SNPS_DB_DICT = None
ENSIMPL_SNPS_DIR = None


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


def get_all_ensimpl_snps_dbs(top_dir):
    """Configure the list of ensimpl snp db files in `directory`.  This will
    set values for :data:`ENSIMPL_SNPS_DBS` and :data:`ENSIMPL_SNPS_DBS_DICT`.

    Args:
        top_dir (str): The directory path.
    """
    version_dict = {}
    for directory in sorted(os.listdir(top_dir)):
        d = os.path.abspath(os.path.join(top_dir, directory))
        if os.path.isdir(d):
            files_in_dir = []
            for file in sorted(os.listdir(d)):
                f = os.path.abspath(os.path.join(d, file))
                if os.path.isfile(f):
                    files_in_dir.append(f)
            if len(files_in_dir):
                version = files_in_dir[0].split('/')[-2]
                for file in files_in_dir:
                    elems = file.split('/')
                    if file[-4:] == '.db3':
                        species = elems[-1].split('.')[-2]
                        k = '{}:{}'.format(version, species)
                        temp = version_dict.get(k, {})
                        temp['db'] = file
                        temp['species'] = species
                        temp['version'] = version
                        version_dict[k] = temp


                    elif file[-3:] == '.gz':
                        if 'musculus' in file.lower():
                            k = '{}:Mm'.format(version)
                            temp = version_dict.get(k, {})
                            temp['vcf'] = file
                            temp['species'] = 'Mm'
                            temp['version'] = version
                            version_dict[k] = temp
                        elif 'sapiens' in file.lower():
                            k = '{}:Hs'.format(version)
                            temp = version_dict.get(k, {})
                            temp['vcf'] = file
                            temp['species'] = 'Mm'
                            temp['version'] = version
                            version_dict[k] = temp

    # sort the databases in descending order by version and than species for
    # readability in the API
    all_sorted_dbs = utils.multikeysort(version_dict.values(),
                                        ['-version', 'species'])

    global ENSIMPL_SNPS_DBS
    ENSIMPL_SNPS_DBS = all_sorted_dbs
    global ENSIMPL_SNPS_DB_DICT
    ENSIMPL_SNPS_DB_DICT = version_dict
    global ENSIMPL_SNPS_DIR
    ENSIMPL_SNPS_DIR = os.path.abspath(directory)


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


