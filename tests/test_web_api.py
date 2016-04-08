import taskclustercoalesce.web as web
import unittest
import json
from mockredis import mock_redis_client
from flask import g
from mock import patch
from contextlib import contextmanager
from flask import appcontext_pushed


class WebTestBase(unittest.TestCase):

    def setUp(self):
        web.app.config['TESTING'] = True
        web.app.config['PREFIX'] = self.pf = 'testing.prefix.'
        self.app = web.app.test_client()
        self.m_rds = mock_redis_client()

    def tearDown(self):
        pass

    @contextmanager
    def set_rds(self, app, m_rds_client):
        ''' Overrides global redis client with mock_redis_client '''
        def handler(sender, **kwargs):
            g.rds = m_rds_client
        with appcontext_pushed.connected_to(handler, app):
            yield

    def ordered(self, obj):
        ''' Recursively sort object '''
        if isinstance(obj, dict):
            return sorted((k, self.ordered(v)) for k, v in obj.items())
        if isinstance(obj, list):
            return sorted(self.ordered(x) for x in obj)
        else:
            return obj


class WebTestCase(WebTestBase):

    def test_root_static(self):
        rv = self.app.get('/')
        actual = json.loads(rv.data)
        expected = {'versions': ['v1']}
        self.assertEqual(self.ordered(actual), self.ordered(expected))
        self.assertEqual(rv.status_code, 200)

    @patch('time.time')
    @patch.object(web, 'starttime', 1.0)
    def test_ping_time(self, m_time):
        m_time.return_value = 2.0
        rv = self.app.get('/v1/ping')
        actual = json.loads(rv.data)
        expected = {'alive': True, 'uptime': 1.0}
        self.assertEqual(self.ordered(actual), self.ordered(expected))
        self.assertEqual(rv.status_code, 200)

    def test_coalesce_key_list_empty(self):
        with self.set_rds(web.app, self.m_rds):
            with web.app.test_client() as c:
                rv = c.get('/v1/list')
                actual = json.loads(rv.data)
                expected = {self.pf: []}
                self.assertEqual(self.ordered(actual), self.ordered(expected))
                self.assertEqual(rv.status_code, 200)

    def test_coalesce_key_list_single(self):
        self.m_rds.sadd(self.pf + "list_keys", "single_key")
        with self.set_rds(web.app, self.m_rds):
            with web.app.test_client() as c:
                rv = c.get('/v1/list')
                actual = json.loads(rv.data)
                expected = {self.pf: ["single_key"]}
                self.assertEqual(self.ordered(actual), self.ordered(expected))
                self.assertEqual(rv.status_code, 200)

    def test_coalesce_key_list_multi(self):
        self.m_rds.sadd(self.pf + 'list_keys', 'key_1', 'key_2', 'key_3')
        with self.set_rds(web.app, self.m_rds):
            with web.app.test_client() as c:
                rv = c.get('/v1/list')
                actual = json.loads(rv.data)
                expected = {self.pf: ['key_1', 'key_2', 'key_3']}
                self.assertEqual(self.ordered(actual), self.ordered(expected))
                self.assertEqual(rv.status_code, 200)

    def test_coalesce_task_list_empty(self):
        with self.set_rds(web.app, self.m_rds):
            with web.app.test_client() as c:
                rv = c.get('/v1/list/sample.key.1')
                actual = json.loads(rv.data)
                expected = {'supersedes': []}
                self.assertEqual(self.ordered(actual), self.ordered(expected))
                self.assertEqual(rv.status_code, 200)

    def test_coalesce_task_list_single(self):
        self.m_rds.lpush(self.pf + 'lists.' + 'sample.key.1', 'taskId1')
        with self.set_rds(web.app, self.m_rds):
            with web.app.test_client() as c:
                rv = c.get('/v1/list/sample.key.1')
                actual = json.loads(rv.data)
                expected = {'supersedes': ['taskId1']}
                self.assertEqual(self.ordered(actual), self.ordered(expected))
                self.assertEqual(rv.status_code, 200)

    def test_coalesce_task_list_multi(self):
        self.m_rds.lpush(self.pf + 'lists.' + 'sample.key.1',
                         'taskId1', 'taskId2', 'taskId3')
        with self.set_rds(web.app, self.m_rds):
            with web.app.test_client() as c:
                rv = c.get('/v1/list/sample.key.1')
                actual = json.loads(rv.data)
                expected = {'supersedes': ['taskId1', 'taskId2', 'taskId3']}
                self.assertEqual(self.ordered(actual), self.ordered(expected))
                self.assertEqual(rv.status_code, 200)

    def test_stats_multi(self):
        stats = {'pending_count': '8',
                 'coalesced_lists': '7',
                 'unknown_tasks': '3',
                 'premature': '4',
                 'total_msgs_handled': '1'
                 }
        self.m_rds.hmset(self.pf + "stats", stats)
        with self.set_rds(web.app, self.m_rds):
            with web.app.test_client() as c:
                rv = c.get('/v1/stats')
                actual = json.loads(rv.data)
                expected = stats
                print(actual)
                print(expected)
                self.assertEqual(self.ordered(actual), self.ordered(expected))
                self.assertEqual(rv.status_code, 200)

if __name__ == '__main__':
    unittest.main()
