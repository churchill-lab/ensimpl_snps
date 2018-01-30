# -*- coding: utf_8 -*-
import sqlite3

import ensimpl_snps.utils as utils
import ensimpl_snps.fetch.utils as fetch_utils

LOG = utils.get_logger()


def meta(version, species_id):
    """Get the database meta information..

    Args:
        version (int): Ensembl version.
        species_id (str): Ensembl species identifier.

    Returns:
        dict: A ``dict`` with the following keys:
            * assembly
            * assembly_patch
            * species
            * version
    """
    sql_meta = '''
        SELECT distinct meta_key meta_key, meta_value, species_id
          FROM meta_info
         WHERE species_id = :species_id 
         ORDER BY meta_key
    '''

    conn = fetch_utils.connect_to_database(version, species_id)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    meta_info = {'species': species_id}

    for row in cursor.execute(sql_meta, {'species_id': species_id}):

        for val in ['version', 'assembly', 'assembly_patch']:
            if row['meta_key'] == val:
                meta_info[val] = row['meta_value']

    cursor.close()

    return meta_info
