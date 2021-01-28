import collections
import ftfy
import json
import pathlib
import re

from csvw import dsv
from pycldf import Source
from clldutils.misc import slug
from cldfbench import CLDFSpec
from pylexibank.dataset import Dataset as BaseDataset
from sqlalchemy import create_engine


exclude_fields = [
    'created',
    'version',
    'updated',
    'active',
    'polymorphic_type',
]


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = 'nts'

    def _query(self, db, q):
        res = db.execute(q)
        header = res.keys()
        r = []
        for row in list(res):
            d = collections.OrderedDict(zip(header, row))
            for k in exclude_fields:
                if k in d:
                    del d[k]
            r.append(d)
        return r

    def _fix_encodings(self, s):
        if s is None:
            return ''
        # fix non-systematic faults
        ch = {
            'YélÌ¨': 'Yélî',
            'YélÃ®': 'Yélî',
            'ÃÂ': 'ē',
            ' Ãf the penu': ' If the penu',
            'Â·': '·',
            'Â ': ' ',
            'Ã': '‘',
            'Ã': '’',
            'Â\u00AD': '-',
            'FranA§ois': 'François',
            'Â§': '§',
            'Ã;': ';',
        }
        for k, v in ch.items():
            s = s.replace(k, v)

        # correct possible multiple encoding faults
        b = bytes(s, 'utf-8')
        c = re.sub(b'\xc3\x83\xc2\xa2\xc3\x82\xc2(.)\xc3\x82\xc2', b'\xe2\\1', b)
        c = re.sub(b'\xc3\x83\xc2\x83\xc3\x82\xc2(.)', b'\xc3\\1', c)
        c = re.sub(b'\xc3\xa2\xc2(.)\xc2', b'\xe2\\1', c)
        c = re.sub(b'\xc3\x82\xc2', b'\xc2', c)
        c = re.sub(b'\xc3\x83\xc2', b'\xc3', c)
        c = re.sub(b'\xc3\x84\xc2', b'\xc4', c)
        c = re.sub(b'\xc3\x85\xc2', b'\xc5', c)
        c = re.sub(b'\xc3\x86\xc2', b'\xc6', c)
        c = re.sub(b'\xc3\x89\xc2', b'\xc9', c)
        c = re.sub(b'\xc3\x8a\xc2', b'\xca', c)
        c = re.sub(b'\xc3\x8c\xc2', b'\xcc', c)
        c = re.sub(b'\xc3\x8d\xc2', b'\xcd', c)

        return ftfy.fix_encoding(c.decode('utf-8'))

    def db_dump_to_csv(self):

        exclude_tables = [
            'alembic_version',
        ]

        dbc = create_engine('postgresql://postgres@/nts')

        for t in self._query(dbc, "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'")[1]:
            table = t[0]
            if table in exclude_tables:
                continue
            header, rows = self._query(dbc, 'select * from {0}'.format(table))
            if len(rows) == 0:
                continue
            print(table)
            with dsv.UnicodeWriter(
                    'raw/{0}.csv'.format(table)) as w:
                h = [c for c in header if c not in exclude_fields]
                for c in h:
                    print('    {0}'.format(c))
                w.writerow(h)
                for row in sorted(rows):
                    d = collections.OrderedDict(zip(header, row))
                    for k in exclude_fields:
                        if k in d:
                            del d[k]
                    w.writerow(d.values())

    def cldf_specs(self):
        return CLDFSpec(module='StructureDataset', dir=self.cldf_dir)

    def cmd_download(self, args):
        self.db_dump_to_csv()

    def cmd_makecldf(self, args):
        # precondition: load locally postgres sql dump /raw/nts-pg_dump.sql.gz as database 'nts'
        with args.writer as ds:
            dbc = create_engine('postgresql://postgres@/nts')

            self.create_schema(args.writer.cldf)

            pk2id = collections.defaultdict(dict)

            for src in self._query(dbc, 'SELECT * FROM source ORDER BY name'):
                pk2id['sources'][src['pk']] = src['id']
                id_ = src['id']
                del src['pk']
                del src['id']
                src_ = collections.OrderedDict()
                for k, v in src.items():
                    if v is not None and v != '{"gbs": {}}':
                        src_[k] = v
                ds.cldf.add_sources(Source(src['bibtex_type'], id_, src_))

            for lg in self._query(
                    dbc,
                    """SELECT lg.pk, lg.id, lg.name, lg.latitude, lg.longitude,
                        array_to_string(array_agg(DISTINCT i.name) FILTER (WHERE length(i.name) = 3), '') AS iso,
                        array_to_string(array_agg(DISTINCT i.name) FILTER (WHERE length(i.name) > 3), '') AS glottocode,
                        nl.macroarea, nl.representation, f.name AS  family,
                        array_to_string(array_agg(DISTINCT s.id), ';') as source_id
                       FROM language AS lg
                       FULL JOIN languageidentifier AS li ON lg.pk = li.language_pk
                       FULL JOIN identifier AS i ON i.pk = li.identifier_pk
                       FULL JOIN languagesource as ls ON lg.pk = ls.language_pk
                       FULL JOIN source AS s ON s.pk = ls.source_pk
                       JOIN ntslanguage AS nl ON lg.pk = nl.pk
                       FULL JOIN family AS f ON nl.family_pk = f.pk
                       GROUP BY lg.pk, lg.id, lg.name, lg.latitude, lg.longitude, nl.macroarea, nl.representation, f.name
                       ORDER BY lg.name"""):
                pk2id['languages'][lg['pk']] = lg['id']
                ds.objects['LanguageTable'].append(dict(
                    ID=lg['id'],
                    Name=lg['name'],
                    Latitude=lg['latitude'],
                    Longitude=lg['longitude'],
                    Glottocode=lg['glottocode'],
                    ISO639P3code=lg['iso'],
                    Macroarea=lg['macroarea'],
                    Representation=lg['representation'],
                    Family=lg['family'],
                    Source_ID=lg['source_id'].split(';'),
                ))

            tg_found = False  # tomguldemann appears twice
            for c in self._query(
                    dbc,
                    """SELECT pk, REGEXP_REPLACE(contributor, '^\\s+', '') AS contributor, domain, pdflink, citation
                       FROM designer ORDER BY REGEXP_REPLACE(contributor, '^\\s+', '')"""):
                id_ = slug(c['contributor'])
                if id_ == 'tomguldemann' and tg_found:
                    id_ = 'tomguldemann-1'
                pk2id['contributors'][c['pk']] = id_
                ds.objects['contributors.csv'].append(dict(
                    ID=id_,
                    Name=c['contributor'],
                    Domain=c['domain'],
                    Link=c['pdflink'],
                    Citation=c['citation'],
                ))
                tg_found = bool(id_ == 'tomguldemann')

            for p in self._query(
                    dbc,
                    """SELECT p.id, p.name, f.*, fd.name AS domain
                       FROM parameter AS p, feature AS f, featuredomain AS fd
                       WHERE p.pk = f.pk AND f.featuredomain_pk = fd.pk
                       ORDER BY f.sortkey_int"""):
                pk2id['parameters'][p['pk']] = p['id']
                ds.objects['ParameterTable'].append(dict(
                    ID=p['id'],
                    Name=p['name'],
                    Contributor_ID=pk2id['contributors'][p['designer_pk']],
                    Doc=p['doc'],
                    Vdoc=p['vdoc'],
                    Name_french=p['name_french'],
                    Clarification=p['clarification'],
                    Alternative_id=p['alternative_id'],
                    Representation=p['representation'],
                    Dependson=p['dependson'],
                    Abbreviation=p['abbreviation'],
                    Jl_relevant_unit=p['jl_relevant_unit'],
                    Jl_function=p['jl_function'],
                    Jl_formal_means=p['jl_formal_means'],
                    Domain=p['domain'],
                ))

            for c in self._query(
                    dbc,
                    """SELECT concat(split_part(id, '-', 1), '-', number) AS id,
                        pk, name, description, parameter_pk, number, jsondata
                       FROM domainelement ORDER BY split_part(id, '-', 1)::INTEGER, number"""):
                pk2id['codes'][c['pk']] = c['id']
                ds.objects['CodeTable'].append(dict(
                    ID=c['id'],
                    Name=c['name'],
                    Description=c['description'],
                    Parameter_ID=pk2id['parameters'][c['parameter_pk']],
                    Number=int(c['number']),
                    Icon=json.loads(c['jsondata'])['icon'],
                ))

            for v in self._query(
                    dbc,
                    """SELECT v.jsondata, v.id, v.pk, vs.language_pk, v.domainelement_pk AS code_id,
                        vs.parameter_pk, de.name AS value, nvs.comment, nvs.contributed_datapoint,
                        vs.source AS source_add,
                        array_agg(DISTINCT vsr.source_pk) AS source_pks
                       FROM value AS v, valueset AS vs
                       FULL JOIN valuesetreference AS vsr ON vsr.valueset_pk = vs.pk,
                       ntsvalue AS nvs, domainelement AS de
                       WHERE v.valueset_pk = vs.pk AND nvs.pk = v.pk AND v.domainelement_pk = de.pk
                       GROUP BY v.pk, vs.language_pk, vs.parameter_pk, de.name,
                        nvs.comment, nvs.contributed_datapoint, vs.source
                       ORDER BY split_part(v.id, '-', 1)::INTEGER, split_part(v.id, '-', 2)"""):

                com_ = v['comment']
                if com_ is not None:
                    com_ = self._fix_encodings(v['comment'])
                    com_ = re.sub(r'\s+', ' ', com_).strip()
                    if com_ == 'Â':
                        com_ = None

                sids = set()
                if v['source_pks'] and len(v['source_pks']):
                    for s in v['source_pks']:
                        if s:
                            sids.add(pk2id['sources'][s])
                ds.objects['ValueTable'].append(dict(
                    ID=v['id'],
                    Language_ID=pk2id['languages'][v['language_pk']],
                    Parameter_ID=pk2id['parameters'][v['parameter_pk']],
                    Code_ID=pk2id['codes'][v['code_id']],
                    Value=v['value'],
                    Comment=com_,
                    Source=sorted(sids),
                    Source_add=self._fix_encodings(v['source_add']).strip(),
                    contributed_datapoint=v['contributed_datapoint'],
                    Icon=json.loads(v['jsondata'])['icon']['icon'],
                ))

    def create_schema(self, cldf):
        cldf.add_component(
            'ParameterTable',
            {'name': 'Contributor_ID', 'dc:description': 'Represents the feature designer'},
            'Doc',
            'Vdoc',
            'Name_french',
            'Clarification',
            'Alternative_id',
            'Representation',
            'Dependson',
            'Abbreviation',
            'Jl_relevant_unit',
            'Jl_function',
            'Jl_formal_means',
            'Domain',
        )
        cldf.add_component(
            'LanguageTable',
            'Family',
            {'name': 'Representation', 'datatype': 'integer'},
            {'name': 'Source_ID', 'separator': ';'},
        )
        t = cldf.add_table(
            'contributors.csv',
            {
                'name': 'ID',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#id',
            },
            {
                'name': 'Name',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#name',
            },
            'Domain',
            'Link',
            'Citation',
            primaryKey=['ID'],
        )
        t.common_props['dc:conformsTo'] = None
        cldf.add_component(
            'CodeTable',
            {'name': 'Number', 'datatype': 'integer'},
            'Icon',
        )
        cldf.add_columns(
            'ValueTable',
            {'name': 'Source_add', 'dc:description': 'Additional Source text field'},
            'contributed_datapoint',
            'Icon',
        )
        cldf.add_foreign_key('ParameterTable', 'Contributor_ID', 'contributors.csv', 'ID')
