# -*- coding: utf-8 -*-
"""This module is specific to ensimpl db operations.

Todo:
    * better documentation
"""
import sqlite3
import time

import ensimpl_snps.utils as utils

LOG = utils.get_logger()


def initialize(db):
    """Initialize the ensimpl_snps database.

    Args:
        db (str): Full path to the database file.
    """
    LOG.info('Initializing database: {}'.format(db))

    start = time.time()
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    LOG.info('Generating tables...')
    for sql in SQL_CREATE_TABLES:
        LOG.debug(sql)
        cursor.execute(sql)

    cursor.close()
    conn.commit()
    conn.close()

    LOG.info('Database initialized in: {}'.format(
        utils.format_time(start, time.time())))


def insert_snps(db, snps):
    """Insert snps into the database.

    Args:
        db (str): Name of the database file.
        snps (list): A ``list`` of snps.
    """
    LOG.info('Inserting snps into database: {}'.format(db))

    start = time.time()
    conn = sqlite3.connect(db)

    sql_snps_insert = ('INSERT INTO snps '
                       'VALUES (?, ?, ?, ?, ?, ?)')

    cursor = conn.cursor()
    LOG.debug('Inserting {:,} snps...'.format(len(snps)))
    cursor.executemany(sql_snps_insert, snps)
    cursor.close()
    conn.commit()
    conn.close()

    LOG.info('SNPs inserted in: {}'.format(
        utils.format_time(start, time.time())))


def finalize(db, ref):
    """Finalize the database.  Move everything to where it needs to be and
    create the necessary indices.

     Args:
        db (str): Name of the database file.

        ref (:obj:`ensimpl_snps.create.create_ensimpl.EnsemblReference`):
            Contains information about the Ensembl reference.
     """
    start = time.time()
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    LOG.info("Finalizing database....")

    sql_meta_insert = 'INSERT INTO meta_info VALUES (null, ?, ?, ?)'

    meta_data = []
    meta_data.append(('version', ref.version, ref.species_id))
    meta_data.append(('assembly', ref.assembly, ref.species_id))
    meta_data.append(('assembly_patch', ref.assembly_patch, ref.species_id))

    cursor.executemany(sql_meta_insert, meta_data)

    LOG.info('Creating indices...')
    for sql in SQL_INDICES:
        LOG.debug(sql)
        cursor.execute(sql)

    conn.row_factory = sqlite3.Row

    LOG.info('Checking...')
    for sql in SQL_SELECT_CHECKS:
        LOG.debug(sql)
        cursor = conn.cursor()

        for row in cursor.execute(sql):
            LOG.info('**** WARNING ****')
            LOG.info(utils.dictify_row(cursor, row))
            break

        cursor.close()

    LOG.info('Information')
    for sql in SQL_SELECT_FINAL_INFO:
        LOG.debug(sql)
        cursor = conn.cursor()

        for row in cursor.execute(sql):
            LOG.info('{}\t{}\t{}'.format(row[0], row[1], row[2]))

        cursor.close()

    conn.commit()
    conn.close()

    LOG.info("Finalizing complete: {0}".format(
        utils.format_time(start, time.time())))


SQL_CREATE_TABLES = ['''
    CREATE TABLE IF NOT EXISTS meta_info (
       meta_info_key INTEGER,
       meta_key TEXT NOT NULL,
       meta_value TEXT NOT NULL,
       species_id TEXT NOT NULL,
       PRIMARY KEY (meta_info_key)
    );
''', '''
    CREATE TABLE IF NOT EXISTS snps (
       chrom TEXT NOT NULL,
       pos INTEGER NOT NULL,
       snp_id TEXT NOT NULL,
       ref TEXT,
       alt TEXT,
       bin INTEGER NOT NULL
    );
''']

SQL_INDICES = [
    'CREATE INDEX IF NOT EXISTS idx_snps_chrom ON snps (chrom ASC);',
    'CREATE INDEX IF NOT EXISTS idx_snps_pos ON snps (pos ASC);',
    'CREATE INDEX IF NOT EXISTS idx_snps_id ON snps (snp_id ASC);',
    'CREATE INDEX IF NOT EXISTS idx_snps_bin ON snps (bin ASC);',
]

SQL_SELECT_FINAL_INFO = [
    '''
SELECT distinct meta_key meta_key, meta_value, species_id
  FROM meta_info
 ORDER BY meta_key       
    '''
]

SQL_SELECT_CHECKS = [
   '''
SELECT count(1), snp_id 
  FROM snps 
 GROUP BY snp_id 
HAVING count(1) > 1
   ''',
   '''
SELECT count(1), chrom, pos 
  FROM snps 
 GROUP BY chrom, pos 
HAVING count(1) > 1
   '''

]
