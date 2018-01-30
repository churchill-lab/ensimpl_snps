# -*- coding: utf-8 -*-
import time

import click

from ensimpl_snps.utils import configure_logging, format_time, get_logger
import ensimpl_snps.create.create_ensimpl_snps as create_ensimpl_snps


@click.command('create', options_metavar='<options>',
             short_help='create an annotation database')
@click.option('-d', '--directory', default='.',
              type=click.Path(file_okay=False, exists=True,
                              resolve_path=True, dir_okay=True))
@click.option('-r', '--resource', default=create_ensimpl_snps.DEFAULT_CONFIG)
@click.option('-s', '--species', multiple=True)
@click.option('--ver', multiple=True)
@click.option('-v', '--verbose', count=True)
def cli(directory, resource, species, ver, verbose):
    """
    Creates a new ensimpl snps database <filename> using Ensembl <version> and species <species>.
    """
    configure_logging(verbose)
    LOG = get_logger()

    if ver:
        ensembl_versions = list(ver)
    else:
        ensembl_versions = None

    if species:
        ensembl_species = list(species)
    else:
        ensembl_species = None

    LOG.info("Creating database...")

    tstart = time.time()
    create_ensimpl_snps.create(ensembl_versions, ensembl_species, directory, resource)
    tend = time.time()

    LOG.info("Creation time: {}".format(format_time(tstart, tend)))

