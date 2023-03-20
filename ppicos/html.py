#!/usr/bin/env python
# https://pythonadventures.wordpress.com/2014/02/25/jinja2-example-for-generating-a-local-file-using-a-template/

import os

from jinja2 import Environment, FileSystemLoader

PATH = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_ENVIRONMENT = Environment(
    autoescape=False,
    loader=FileSystemLoader(os.path.join(PATH, 'html_templates')),
    trim_blocks=False)


def render_template(template_filename, context):
    return TEMPLATE_ENVIRONMENT.get_template(template_filename).render(context)


def make_file_overview(filetype, site_html_outdir, run_id, table, run_date, settings_dict):
    outfile = os.path.join(site_html_outdir, "ppicos_file_report_{}.html".format(filetype))

    context = {
        'filetype': filetype, 'RUN_ID': run_id, 'table': table,
        'run_date': run_date, 'settings': settings_dict
    }

    with open(outfile, 'w') as f:
        html = render_template('file_overview.html', context)
        f.write(html)
