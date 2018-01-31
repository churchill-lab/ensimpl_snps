# -*- coding: utf_8 -*-
import re
import sqlite3
import time

import ensimpl_snps.utils as utils
import ensimpl_snps.fetch.utils as fetch_utils

LOG = utils.get_logger()

REGEX_SNP_ID = re.compile("rs[0-9]{1,}", re.IGNORECASE)
REGEX_REGION = re.compile("(CHR|)*\s*([0-9]{1,2}|X|Y|MT)\s*(-|:)?\s*(\d+)\s*(MB|M|K|)?\s*(-|:|)?\s*(\d+|)\s*(MB|M|K|)?", re.IGNORECASE)


def by_ids(ids, version, species):
    """Perform the search for ids.

    Args:
        ids (list): A ``list`` of ids to look for.
        version (int): The Ensembl version.
        species (str): The Ensembl species identifier.

    Returns:
        dict: A ``dict`` withe keys return ``snps`` and ``snps_not_found``.

    Raises:
        ValueError: When `ids` is empty.
    """
    LOG.debug('ids={} ...'.format(ids[0:max(len(ids), 10)]))
    LOG.debug('version={}'.format(version))
    LOG.debug('species={}'.format(species))

    try:
        conn = fetch_utils.connect_to_database(version, species)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if not ids:
            raise ValueError('no ids were passed in')

        temp_table = 'lookup_ids_{}'.format(utils.create_random_string())

        # create a temp table and insert into
        SQL_TEMP = ('CREATE TEMPORARY TABLE {} ( '
                    'query_id TEXT, '
                    'PRIMARY KEY (query_id) '
                    ');').format(temp_table)

        cursor.execute(SQL_TEMP)

        SQL_TEMP = 'INSERT INTO {} VALUES (?);'.format(temp_table)
        query_ids = [(_,) for _ in ids]
        cursor.executemany(SQL_TEMP, query_ids)

        SQL_QUERY = ('SELECT s.* '
                     '  FROM snps s '
                     ' WHERE s.snp_id IN (SELECT distinct query_id FROM {}) '
                     ' ORDER BY s.chrom, s.pos').format(temp_table)

        snps = []
        snp_ids = []
        for row in cursor.execute(SQL_QUERY):
            snps.append([
                row['chrom'],
                row['pos'],
                row['snp_id'],
                row['ref'],
                row['alt']
            ])
            snp_ids.append(row['snp_id'])

        cursor.close()
        conn.close()

        snps_found = set(snp_ids)
        snps_not_found = [x for x in ids if x not in snps_found]

        return {'snps': snps, 'snps_not_found': snps_not_found}

    except Exception as e:
        LOG.error('Error: {}'.format(e))
        return None


def by_region(region, version, species, limit=None):
    """Perform the search by region.

    Args:
        region (str): The region to look for SNPs.
        version (int): The Ensembl version number.
        species (str): The Ensembl species identifier.
        limit (int, optional): Maximum number of SNPs to return, ``None`` for
            all.

    Returns:
        list: All the SNPs in `region`.  Each element is another ``list`` with
        the following values:
            * chromosome
            * position
            * SNP identifier
            * reference allele
            * alternate allele

    Raises:
        ValueError: When `region` is empty.
    """
    LOG.debug('range={}'.format(region))
    LOG.debug('version={}'.format(version))
    LOG.debug('species_id={}'.format(species))
    LOG.debug('limit={}'.format(limit))

    try:
        conn = fetch_utils.connect_to_database(version, species)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if not region:
            raise ValueError('no ids were passed in')

        new_region = fetch_utils.str_to_region(region)
        bins = utils.bins(new_region.start_position,
                          new_region.end_position,
                          one=False)

        SQL_QUERY = ('SELECT s.* '
                     '  FROM snps s '
                     ' WHERE s.chrom = ? '
                     '   AND s.pos >= ? '
                     '   AND s.pos <= ? '
                     '   AND s.bin IN ({}) '
                     ' ORDER BY s.pos').format(','.join('?'*len(bins)))

        parameters = [
            new_region.chromosome,
            new_region.start_position,
            new_region.end_position
        ]
        parameters.extend(list(bins))

        LOG.info('Query: {}'.format(SQL_QUERY))
        LOG.info('Parameters: {}'.format(parameters))

        start_time = time.time()

        snps = []
        for row in cursor.execute(SQL_QUERY, parameters):
            snps.append([
                row['chrom'],
                row['pos'],
                row['snp_id'],
                row['ref'],
                row['alt']
            ])

        LOG.info('Done: {}'.format(utils.format_time(start_time, time.time())))

        cursor.close()
        conn.close()

        return snps
    except Exception as e:
        LOG.error('Error: {}'.format(e))
        return None
