import hashlib
import json
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'Avance1_P1_Lumina_Ingesta_Storage'))
from exploration_data import build_exploration_query, DIMENSIONS, METRICS, pack_rows


class ExplorationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report=json.loads((ROOT/'P3/evidencias/datos_existentes_latest.json').read_text(encoding='utf-8'))

    def test_sql_is_readonly_and_matches_executed_query(self):
        r=self.report
        sql=build_exploration_query(r['source_table']['table_id'],[f['name'] for f in r['source_table']['schema']])
        self.assertTrue(sql.startswith('SELECT'))
        for token in ['INSERT ', 'CREATE ', 'DELETE ', 'RAND(', 'LIMIT ']:
            self.assertNotIn(token,sql)
        self.assertEqual(hashlib.sha256(sql.encode()).hexdigest(),r['exploration']['sql_sha256'])
        self.assertEqual(sql,r['exploration']['query_sql'])

    def test_missing_dimensions_and_injection_rejected(self):
        with self.assertRaises(ValueError): build_exploration_query('a.b.c',[])
        with self.assertRaises(ValueError): build_exploration_query('a.b.c; DROP TABLE c',DIMENSIONS+METRICS)

    def test_dictionary_roundtrip_real_export(self):
        x=self.report['exploration']
        unpacked=[{k:x['dictionaries'][k][v] if k in DIMENSIONS else v
                   for k,v in zip(x['columns'],values)} for values in x['rows'][:20]]
        packed=pack_rows(unpacked)
        for values,original in zip(packed['rows'],unpacked):
            reconstructed={k:packed['dictionaries'][k][v] if k in DIMENSIONS else v
                           for k,v in zip(packed['columns'],values)}
            self.assertEqual(original,reconstructed)


if __name__=='__main__': unittest.main()
