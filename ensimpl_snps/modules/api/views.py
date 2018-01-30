# -*- coding: utf-8 -*-
from functools import wraps

from flask import Blueprint
from flask import current_app
from flask import jsonify
from flask import request

import ensimpl_snps.db_config as db_config

from ensimpl_snps.fetch import get
from ensimpl_snps.fetch import search as search_ensimpl

api = Blueprint('api', __name__, template_folder='templates', url_prefix='/api')


def support_jsonp(func):
    """Wraps JSONified output for JSONP requests."""

    @wraps(func)
    def decorated_function(*args, **kwargs):
        callback = request.args.get('callback', False)
        if callback:
            resp = func(*args, **kwargs)
            resp.set_data('{}({})'.format(str(callback),
                                          resp.get_data(as_text=True)))
            resp.mimetype = 'application/javascript'
            return resp
        else:
            return func(*args, **kwargs)

    return decorated_function


@api.route("/versions")
@support_jsonp
def versions():
    """Get the version and species information.

    No parameters are needed.

    If successful, a JSON response will be returned with a single
    ``version`` element containing a ``list`` of versions consisting of the
    following items:

    ==============  =======  ==================================================
    Param           Type     Description
    ==============  =======  ==================================================
    version         integer  the Ensembl version number
    species         string   the species identifier (example 'Hs', 'Mm')
    assembly        string   the genome assembly information
    assembly_patch  string   the genome assembly patch number
    ==============  =======  ==================================================

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    version_info = []

    try:
        for it in db_config.ENSIMPL_SNPS_DBS:
            meta = get.meta(it['version'], it['species'])
            version_info.append(meta)
    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify({'versions': version_info})


@api.route("/snps", methods=['GET', 'POST'])
@support_jsonp
def snps():
    """Get SNPs for a particular Ensembl version and species.

    The following is a list of the valid parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    version  integer  the Ensembl version number
    species  string   the species identifier (example 'Hs', 'Mm')
    ids      list     a list of ids to find
    =======  =======  ===================================================

    If successful, a JSON response will be returned with the following elements:

    ==============  =======  ==================================================
    Element         Type     Description
    ==============  =======  ==================================================
    num_snps        integer  the number of snps found
    snps            list     a list of snps, each element contains snp data
    num_unknown     integer  the number of snp ids not found
    unknown         list     a list of the snp ids not found
    ==============  =======  ==================================================

    The elements in the snp data are:
        * chromosome
        * position
        * SNP identifier
        * reference allele
        * alternate allele

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug('Call for: GET {}'.format(request.url))

    version = request.values.get('version', None)
    species = request.values.get('species', None)
    requested_ids = request.values.getlist('ids', None)

    ret = {
        'num_snps': 0,
        'snps': None,
        'num_unknown': 0,
        'unknown': None
    }

    try:
        if not version:
            raise ValueError('No version specified')

        if not species:
            raise ValueError('No species specified')

        result = search_ensimpl.by_ids(requested_ids, version, species)
        snps = result['snps']
        snps_not_found = result['snps_not_found']

        ret['num_snps'] = len(snps)
        ret['snps'] = snps
        ret['num_unknown'] = len(snps_not_found)
        ret['unknown'] = snps_not_found

    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


@api.route("/region", methods=['GET', 'POST'])
@support_jsonp
def region():
    """Get SNPs for a particular Ensembl version and species.

    The following is a list of the valid parameters:

    =======  =======  ===================================================
    Param    Type     Description
    =======  =======  ===================================================
    version  integer  the Ensembl version number
    species  string   the species identifier (example 'Hs', 'Mm')
    region   string   a region like "1:10000000-10500000"
    limit    string   max number of items to return, defaults to 100,000
    =======  =======  ===================================================

    If successful, a JSON response will be returned with the following elements:

    ==============  =======  ==================================================
    Element         Type     Description
    ==============  =======  ==================================================
    num_snps        integer  the number of snps found
    snps            list     a list of snps, each element contains snp data
    ==============  =======  ==================================================

    The elements in the snp data are:
        * chromosome
        * position
        * SNP identifier
        * reference allele
        * alternate allele

    If an error occurs, a JSON response will be sent back with just one
    element called ``message`` along with a status code of 500.

    Returns:
        :class:`flask.Response`: The response which is a JSON response.
    """
    current_app.logger.debug('Call for: GET {}'.format(request.url))

    version = request.values.get('version', None)
    species = request.values.get('species', None)
    region = request.values.get('region', None)
    limit = request.values.get('limit', '100000')

    try:
        limit = int(limit)
    except ValueError as ve:
        limit = 100000
        current_app.logger.info(ve)

    ret = {
        'num_snps': 0,
        'snps': None,
    }

    try:
        if not version:
            raise ValueError('No version specified')

        if not species:
            raise ValueError('No species specified')

        snps = search_ensimpl.by_region(region, version, species, limit)

        ret['num_snps'] = len(snps)
        ret['snps'] = snps

        print(ret)

    except Exception as e:
        response = jsonify(message=str(e))
        response.status_code = 500
        return response

    return jsonify(ret)


