# -*- coding: utf-8 -*-
from collections import namedtuple

import io
import os
import time

from pysam import VariantFile

import ensimpl_snps.create.ensimpl_db as ensimpl_db
import ensimpl_snps.utils as utils

DEFAULT_CONFIG = 'ftp://ftp.jax.org/churchill-lab/ensimpl/ensimpl_snps.ensembl.conf'

ENSEMBL_FIELDS = ['version', 'release_date', 'assembly', 'assembly_patch',
                  'species_id', 'species_name', 'vcf_file']
EnsemblReference = namedtuple('EnsemblReference', ENSEMBL_FIELDS)


LOG = utils.get_logger()


def parse_config(resource_name):
    """Take a resource string (file name, url) and open it.  Parse the file.

    Args:
        resource_name (str): the name of the resource

    Returns:
        dict: :obj:`EnsemblReference` with the
            key being the Ensembl version
    """
    start = time.time()
    all_releases = {}
    line = ''

    try:
        with utils.open_resource(resource_name) as fd:
            buffer = io.BufferedReader(fd)
            line = buffer.readline()  # skip first line

            # parse each line and create an EnsemblReference
            for line in buffer:
                line = str(line, 'utf-8')
                elems = line.strip().split('\t')
                if len(elems) == 10:
                    elems.append(None)
                reference = EnsemblReference(*elems)
                release = all_releases.get(reference.version, {})
                release[reference.species_id] = reference
                all_releases[reference.version] = release

        LOG.info('Config parsed in {}'.format(
                utils.format_time(start, time.time())))

    except IOError as io_error:
        LOG.error('Unable to access resource: {}'.format(resource_name))
        LOG.debug(io_error)
        all_releases = None
    except TypeError as type_error:
        LOG.error('Unable to parse resource: {}'.format(resource_name))
        LOG.debug(type_error)
        LOG.debug('Error on the following:')
        LOG.debug(line)
        all_releases = None

    return all_releases


def parseSNPs(db, reference):
    """Create ensimpl database(s).

    Args:
        db (str): Ensembl databse.
        reference (EnsemblReference): Reference information for the Ensembl
            information.
    """
    try:
        # TODO: hardcoded for files right now
        vcf_in = VariantFile(reference.vcf_file[7:])

        snps = []
        idx = 0
        for rec in vcf_in.fetch():
            snps.append([rec.contig, rec.pos, rec.id, rec.ref,
                         ','.join(rec.alts) if rec.alts else '',
                         utils.bins(rec.pos, rec.pos+1, one=True)])

            idx += 1

            if idx == 1000000:
                idx = 0
                ensimpl_db.insert_snps(db, snps)
                snps = []

        ensimpl_db.insert_snps(db, snps)
    except Exception as e:
        print(str(e))
        LOG.error('Unable to parse file: {}'.format(reference.vcf_file[7:]))


def create(ensembl, species, directory, resource):
    """Create Ensimpl SNPs database(s).  Output database name will be:

    "ensembl_snps. ``version`` . ``species`` .db3"

    Args:
        ensembl (list): A ``list`` of all Ensembl versions to create, ``None``
            for all.
        species (list): A ``list`` of all species to create, ``None`` for all.
        directory (str): Output directory.
        resource (str): Configuration file location to parse.
    """
    if ensembl:
        LOG.debug('Ensembl Versions: {}'.format(','.join(ensembl)))
    else:
        LOG.debug('Ensembl Versions: ALL')

    LOG.debug('Directory: {}'.format(directory))
    LOG.debug('Resource: {}'.format(resource))

    releases = parse_config(resource)

    if not releases:
        LOG.error('Unable to determine the Ensembl releases and locations '
                  'for download')
        LOG.error('Please make sure that the resource "{}" is accessible '
                  'and in the correct format'.format(resource))
        raise Exception("Unable to create databases")

    if ensembl:
        all_releases = list(releases.keys())
        all_releases.sort()
        not_found = list(set(ensembl) - set(all_releases))
        if not_found:
            not_found.sort()
            LOG.error('Unable to determine the Ensembl release '
                      'specified: {}'.format(', '.join(not_found)))
            LOG.error('Please make sure that the resource "{}" is '
                      'accessible and in the correct format'.format(resource))
            LOG.error('Found Ensembl versions:'
                      ' {}'.format(', '.join(all_releases)))
            raise Exception("Unable to create databases")

    for release_version, release_value in sorted(releases.items()):
        if ensembl and release_version not in ensembl:
            continue

        for species_id, ensembl_reference in sorted(release_value.items()):
            if not species or (species_id in species):
                LOG.warn('Generating ensimpl database for '
                         'Ensembl version: {}'.format(release_version))

                ensimpl_file = 'ensimpl_snps.{}.{}.db3'.format(release_version,
                                                               species_id)
                ensimpl_file = os.path.join(directory, ensimpl_file)
                utils.delete_file(ensimpl_file)
    
                LOG.info('Creating: {}'.format(ensimpl_file))
                ensimpl_db.initialize(ensimpl_file)

                LOG.info('Extracting and inserting snps...')
                parseSNPs(ensimpl_file, ensembl_reference)
    
                LOG.info('Finalizing...')
                ensimpl_db.finalize(ensimpl_file, ensembl_reference)

        LOG.info('DONE')


